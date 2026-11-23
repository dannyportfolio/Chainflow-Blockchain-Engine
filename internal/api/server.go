package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/security"
)

const maxRequestBodyBytes = 64 * 1024

type contextKey string

const (
	requestIDContextKey contextKey = "request_id"
	principalContextKey contextKey = "principal"
)

type HandlerDeps struct {
	Intents    *application.IntentService
	Auth       *security.APIKeyAuthenticator
	Limiter    *security.TenantRateLimiter
	Logger     *slog.Logger
	ReadyCheck func(context.Context) error
	Now        func() time.Time
}

type submitTransactionRequest struct {
	ChainID          int64   `json:"chain_id"`
	From             string  `json:"from"`
	To               *string `json:"to"`
	ValueWei         string  `json:"value_wei"`
	Data             string  `json:"data"`
	GasLimitOverride *uint64 `json:"gas_limit_override"`
	MaxFeeCapWei     *string `json:"max_fee_cap_wei"`
	Priority         int16   `json:"priority"`
	MaxAttempts      int     `json:"max_attempts"`
	ExpiresAt        *string `json:"expires_at"`
}

type submitTransactionResponse struct {
	IntentID  string `json:"intent_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type cancelTransactionRequest struct {
	Reason string `json:"reason"`
}

type intentDetailResponse struct {
	IntentID          string                     `json:"intent_id"`
	TenantID          string                     `json:"tenant_id"`
	IdempotencyKey    string                     `json:"idempotency_key"`
	ChainID           int64                      `json:"chain_id"`
	From              string                     `json:"from"`
	To                *string                    `json:"to,omitempty"`
	ValueWei          string                     `json:"value_wei"`
	Status            string                     `json:"status"`
	ExecutionOutcome  *string                    `json:"execution_outcome,omitempty"`
	ActiveExecutionID string                     `json:"active_execution_id,omitempty"`
	AttemptCount      int                        `json:"attempt_count"`
	Version           int64                      `json:"version"`
	ExpiresAt         string                     `json:"expires_at"`
	CreatedAt         string                     `json:"created_at"`
	UpdatedAt         string                     `json:"updated_at"`
	Events            []stateEventResponse       `json:"events"`
	Executions        []executionSummaryResponse `json:"executions,omitempty"`
}

type stateEventResponse struct {
	FromStatus string `json:"from_status,omitempty"`
	ToStatus   string `json:"to_status"`
	Actor      string `json:"actor"`
	Reason     string `json:"reason,omitempty"`
	OccurredAt string `json:"occurred_at"`
}

type executionSummaryResponse struct {
	ID            string  `json:"id"`
	AttemptNumber int     `json:"attempt_number"`
	Nonce         uint64  `json:"nonce"`
	TxHash        *string `json:"tx_hash,omitempty"`
	Status        string  `json:"status"`
}

type errorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}

type server struct {
	deps HandlerDeps
}

func NewHandler(deps HandlerDeps) http.Handler {
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	s := &server{deps: deps}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("POST /v1/transactions", s.withAuth(http.HandlerFunc(s.handleSubmitTransaction)))
	mux.Handle("GET /v1/transactions/{id}", s.withAuth(http.HandlerFunc(s.handleGetTransaction)))
	mux.Handle("POST /v1/transactions/{id}/cancel", s.withAuth(http.HandlerFunc(s.handleCancelTransaction)))

	return s.withRequestID(s.withLogging(s.withRecovery(mux)))
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.deps.ReadyCheck != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.deps.ReadyCheck(ctx); err != nil {
			writeError(w, r, http.StatusServiceUnavailable, "service not ready")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *server) handleSubmitTransaction(w http.ResponseWriter, r *http.Request) {
	principal := principalFromContext(r.Context())
	idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idemKey == "" || len(idemKey) > 128 {
		writeError(w, r, http.StatusBadRequest, "Idempotency-Key header is required (max 128 chars)")
		return
	}

	var body submitTransactionRequest
	if err := decodeStrictJSON(w, r, &body); err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	fromAddr, err := domain.ParseAddress(body.From)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if !s.deps.Auth.CanUseSender(principal, fromAddr) {
		writeError(w, r, http.StatusForbidden, "sender address not permitted for API key")
		return
	}

	var toAddr *domain.Address
	if body.To != nil && strings.TrimSpace(*body.To) != "" {
		parsedTo, err := domain.ParseAddress(*body.To)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, err.Error())
			return
		}
		toAddr = &parsedTo
	}

	valueWei := big.NewInt(0)
	if strings.TrimSpace(body.ValueWei) != "" {
		parsedVal, ok := new(big.Int).SetString(strings.TrimSpace(body.ValueWei), 10)
		if !ok || parsedVal.Sign() < 0 {
			writeError(w, r, http.StatusBadRequest, "invalid value_wei")
			return
		}
		valueWei = parsedVal
	}

	var maxFeeCapWei *big.Int
	if body.MaxFeeCapWei != nil && strings.TrimSpace(*body.MaxFeeCapWei) != "" {
		parsedCap, ok := new(big.Int).SetString(strings.TrimSpace(*body.MaxFeeCapWei), 10)
		if !ok || parsedCap.Sign() <= 0 {
			writeError(w, r, http.StatusBadRequest, "invalid max_fee_cap_wei")
			return
		}
		maxFeeCapWei = parsedCap
	}

	calldata, err := decodeHexCalldata(body.Data)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	var expiresAt *time.Time
	if body.ExpiresAt != nil && strings.TrimSpace(*body.ExpiresAt) != "" {
		parsedTime, err := time.Parse(time.RFC3339, strings.TrimSpace(*body.ExpiresAt))
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid expires_at timestamp (RFC3339 required)")
			return
		}
		expiresAt = &parsedTime
	}

	res, err := s.deps.Intents.Submit(r.Context(), application.SubmitIntentInput{
		TenantID:         principal.TenantID,
		IdempotencyKey:   idemKey,
		ChainID:          body.ChainID,
		From:             fromAddr,
		To:               toAddr,
		ValueWei:         valueWei,
		Calldata:         calldata,
		GasLimitOverride: body.GasLimitOverride,
		MaxFeeCapWei:     maxFeeCapWei,
		Priority:         body.Priority,
		MaxAttempts:      body.MaxAttempts,
		ExpiresAt:        expiresAt,
		TraceID:          requestIDFromContext(r.Context()),
	})
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	status := http.StatusOK
	if res.Created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, submitTransactionResponse{
		IntentID:  res.Intent.ID,
		Status:    string(res.Intent.Status),
		CreatedAt: res.Intent.CreatedAt.Format(time.RFC3339),
	})
}

func (s *server) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	principal := principalFromContext(r.Context())
	intentID := r.PathValue("id")
	if intentID == "" {
		writeError(w, r, http.StatusBadRequest, "missing transaction id")
		return
	}

	detail, err := s.deps.Intents.Get(r.Context(), principal.TenantID, intentID)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	var toStr *string
	if detail.Intent.To != nil {
		v := detail.Intent.To.String()
		toStr = &v
	}
	var outcomeStr *string
	if detail.Intent.ExecutionOutcome != nil {
		v := string(*detail.Intent.ExecutionOutcome)
		outcomeStr = &v
	}

	events := make([]stateEventResponse, 0, len(detail.Events))
	for _, ev := range detail.Events {
		events = append(events, stateEventResponse{
			FromStatus: string(ev.FromStatus),
			ToStatus:   string(ev.ToStatus),
			Actor:      ev.Actor,
			Reason:     ev.Reason,
			OccurredAt: ev.OccurredAt.Format(time.RFC3339),
		})
	}

	execs := make([]executionSummaryResponse, 0, len(detail.Executions))
	for _, ex := range detail.Executions {
		var txHashStr *string
		if ex.TxHash != nil {
			v := ex.TxHash.String()
			txHashStr = &v
		}
		execs = append(execs, executionSummaryResponse{
			ID:            ex.ID,
			AttemptNumber: ex.AttemptNumber,
			Nonce:         ex.Nonce,
			TxHash:        txHashStr,
			Status:        string(ex.Status),
		})
	}

	writeJSON(w, http.StatusOK, intentDetailResponse{
		IntentID:          detail.Intent.ID,
		TenantID:          detail.Intent.TenantID,
		IdempotencyKey:    detail.Intent.IdempotencyKey,
		ChainID:           detail.Intent.ChainID,
		From:              detail.Intent.From.String(),
		To:                toStr,
		ValueWei:          detail.Intent.ValueWei.String(),
		Status:            string(detail.Intent.Status),
		ExecutionOutcome:  outcomeStr,
		ActiveExecutionID: detail.Intent.ActiveExecutionID,
		AttemptCount:      detail.Intent.AttemptCount,
		Version:           detail.Intent.Version,
		ExpiresAt:         detail.Intent.ExpiresAt.Format(time.RFC3339),
		CreatedAt:         detail.Intent.CreatedAt.Format(time.RFC3339),
		UpdatedAt:         detail.Intent.UpdatedAt.Format(time.RFC3339),
		Events:            events,
		Executions:        execs,
	})
}

func (s *server) handleCancelTransaction(w http.ResponseWriter, r *http.Request) {
	principal := principalFromContext(r.Context())
	intentID := r.PathValue("id")
	if intentID == "" {
		writeError(w, r, http.StatusBadRequest, "missing transaction id")
		return
	}

	var body cancelTransactionRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := decodeStrictJSON(w, r, &body); err != nil {
			writeError(w, r, http.StatusBadRequest, err.Error())
			return
		}
	}
	if strings.TrimSpace(body.Reason) == "" {
		body.Reason = "caller_cancelled"
	}

	intent, err := s.deps.Intents.Cancel(r.Context(), principal.TenantID, intentID, body.Reason)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"intent_id": intent.ID,
		"status":    string(intent.Status),
	})
}

func (s *server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.deps.Auth == nil {
			writeError(w, r, http.StatusInternalServerError, "authenticator not configured")
			return
		}
		token := extractAPIKey(r)
		principal, err := s.deps.Auth.Authenticate(token)
		if err != nil {
			writeError(w, r, http.StatusUnauthorized, "invalid or missing API key")
			return
		}
		if s.deps.Limiter != nil && !s.deps.Limiter.Allow(principal.TenantID) {
			writeError(w, r, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		ctx := context.WithValue(r.Context(), principalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if reqID == "" || len(reqID) > 64 {
			generated, err := application.NewUUIDv7(s.deps.Now())
			if err != nil {
				generated = "req-fallback"
			}
			reqID = generated
		}
		w.Header().Set("X-Request-ID", reqID)
		ctx := context.WithValue(r.Context(), requestIDContextKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

func (s *server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.deps.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.deps.Logger.Info("http_request",
			"request_id", requestIDFromContext(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

func (s *server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.deps.Logger.Error("http_panic_recovered",
					"request_id", requestIDFromContext(r.Context()),
					"panic", rec,
				)
				writeError(w, r, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *server) writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidInput), errors.Is(err, domain.ErrFeeCapExceeded), errors.Is(err, domain.ErrIntentExpired):
		writeError(w, r, http.StatusBadRequest, err.Error())
	case errors.Is(err, application.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "transaction intent or wallet not found")
	case errors.Is(err, application.ErrIdempotencyConflict),
		errors.Is(err, domain.ErrInvalidTransition),
		errors.Is(err, domain.ErrTerminalState),
		errors.Is(err, domain.ErrVersionConflict):
		writeError(w, r, http.StatusConflict, err.Error())
	case errors.Is(err, application.ErrWalletNotActive):
		writeError(w, r, http.StatusUnprocessableEntity, err.Error())
	default:
		s.deps.Logger.Error("internal_request_error", "request_id", requestIDFromContext(r.Context()), "error", err)
		writeError(w, r, http.StatusInternalServerError, "internal server error")
	}
}

func extractAPIKey(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-API-Key")); v != "" {
		return v
	}
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(authz), "bearer ") {
		return strings.TrimSpace(authz[7:])
	}
	return ""
}

func decodeStrictJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("invalid JSON request body")
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("unexpected trailing JSON content")
	}
	return nil
}

func decodeHexCalldata(raw string) ([]byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "0x" || trimmed == "0X" {
		return nil, nil
	}
	if !strings.HasPrefix(trimmed, "0x") && !strings.HasPrefix(trimmed, "0X") {
		return nil, errors.New("calldata must start with 0x")
	}
	b, err := hex.DecodeString(trimmed[2:])
	if err != nil {
		return nil, errors.New("invalid hex calldata")
	}
	return b, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	writeJSON(w, status, errorResponse{
		Error:     msg,
		RequestID: requestIDFromContext(r.Context()),
	})
}

func requestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDContextKey).(string); ok {
		return v
	}
	return ""
}

func principalFromContext(ctx context.Context) security.TenantPrincipal {
	if v, ok := ctx.Value(principalContextKey).(security.TenantPrincipal); ok {
		return v
	}
	return security.TenantPrincipal{}
}
