package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/api"
	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/persistence/memory"
	"github.com/chainflow/chainflow/internal/security"
)

func setupTestAPI(t *testing.T, readyErr error, ratePerSec float64, burst int) (http.Handler, domain.Address, domain.Address) {
	t.Helper()

	store := memory.NewStore()
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	from, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
	to, _ := domain.ParseAddress("0x2222222222222222222222222222222222222222")

	_ = store.UpsertWallet(context.Background(), domain.Wallet{
		ID:                "01934a1a-0000-7000-8000-999999999999",
		ChainID:           11155111,
		Address:           from,
		Label:             "sepolia-1",
		SignerKeyRef:      "dev:1",
		Status:            domain.WalletActive,
		MaxInflightNonces: 1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, 0)

	svc := application.NewIntentService(application.IntentServiceConfig{
		Intents:             store,
		Wallets:             store,
		ChainID:             11155111,
		DefaultMaxFeeCapWei: big.NewInt(100_000_000_000),
		DefaultTTL:          15 * time.Minute,
		DefaultMaxAttempts:  5,
		Now:                 func() time.Time { return now },
	})

	auth, err := security.NewAPIKeyAuthenticator([]security.APIKeyEntry{
		{
			TenantID:       "tenant-a",
			RawKey:         "key-alpha",
			AllowedSenders: []domain.Address{from},
		},
		{
			TenantID: "tenant-b",
			RawKey:   "key-beta",
		},
	})
	if err != nil {
		t.Fatalf("authenticator init: %v", err)
	}

	limiter := security.NewTenantRateLimiter(ratePerSec, burst, func() time.Time { return now })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	handler := api.NewHandler(api.HandlerDeps{
		Intents: svc,
		Auth:    auth,
		Limiter: limiter,
		Logger:  logger,
		ReadyCheck: func(context.Context) error {
			return readyErr
		},
	})
	return handler, from, to
}

func TestAPI_HealthAndReadiness(t *testing.T) {
	t.Parallel()

	okHandler, _, _ := setupTestAPI(t, nil, 100, 100)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	okHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 /healthz, got %d", rec.Code)
	}

	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recReady := httptest.NewRecorder()
	okHandler.ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Fatalf("expected 200 /readyz, got %d", recReady.Code)
	}

	downHandler, _, _ := setupTestAPI(t, errors.New("db unreachable"), 100, 100)
	recDown := httptest.NewRecorder()
	downHandler.ServeHTTP(recDown, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recDown.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 /readyz when unhealthy, got %d", recDown.Code)
	}
}

func TestAPI_SubmitGetCancelFlowAndValidation(t *testing.T) {
	t.Parallel()

	handler, from, to := setupTestAPI(t, nil, 100, 100)

	unauthReq := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(`{}`))
	unauthRec := httptest.NewRecorder()
	handler.ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without api key, got %d", unauthRec.Code)
	}

	bodyMap := map[string]any{
		"chain_id":  11155111,
		"from":      from.String(),
		"to":        to.String(),
		"value_wei": "1000000",
		"data":      "0xdeadbeef",
	}
	payload, _ := json.Marshal(bodyMap)

	missingIdem := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(payload))
	missingIdem.Header.Set("Authorization", "Bearer key-alpha")
	missingRec := httptest.NewRecorder()
	handler.ServeHTTP(missingRec, missingIdem)
	if missingRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without Idempotency-Key, got %d", missingRec.Code)
	}

	badSenderBody, _ := json.Marshal(map[string]any{
		"chain_id":  11155111,
		"from":      to.String(),
		"to":        to.String(),
		"value_wei": "1",
	})
	forbiddenReq := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(badSenderBody))
	forbiddenReq.Header.Set("X-API-Key", "key-alpha")
	forbiddenReq.Header.Set("Idempotency-Key", "k-forbidden")
	forbiddenRec := httptest.NewRecorder()
	handler.ServeHTTP(forbiddenRec, forbiddenReq)
	if forbiddenRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for unauthorized sender, got %d", forbiddenRec.Code)
	}

	unknownFieldReq := httptest.NewRequest(http.MethodPost, "/v1/transactions", strings.NewReader(`{"unknown_field":1}`))
	unknownFieldReq.Header.Set("X-API-Key", "key-alpha")
	unknownFieldReq.Header.Set("Idempotency-Key", "k-unknown")
	unknownFieldRec := httptest.NewRecorder()
	handler.ServeHTTP(unknownFieldRec, unknownFieldReq)
	if unknownFieldRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown json field, got %d", unknownFieldRec.Code)
	}

	req1 := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(payload))
	req1.Header.Set("X-API-Key", "key-alpha")
	req1.Header.Set("Idempotency-Key", "idem-http-1")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec1.Code, rec1.Body.String())
	}
	if rec1.Header().Get("X-Request-ID") == "" {
		t.Fatal("expected X-Request-ID response header")
	}

	var submitResp struct {
		IntentID string `json:"intent_id"`
		Status   string `json:"status"`
	}
	if err := json.Unmarshal(rec1.Body.Bytes(), &submitResp); err != nil {
		t.Fatalf("unmarshal submit response: %v", err)
	}
	if submitResp.IntentID == "" || submitResp.Status != "QUEUED" {
		t.Fatalf("unexpected submit response: %+v", submitResp)
	}

	reqReplay := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(payload))
	reqReplay.Header.Set("X-API-Key", "key-alpha")
	reqReplay.Header.Set("Idempotency-Key", "idem-http-1")
	recReplay := httptest.NewRecorder()
	handler.ServeHTTP(recReplay, reqReplay)
	if recReplay.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on idempotent replay, got %d", recReplay.Code)
	}

	mutatedBody, _ := json.Marshal(map[string]any{
		"chain_id":  11155111,
		"from":      from.String(),
		"to":        to.String(),
		"value_wei": "999",
		"data":      "0xdeadbeef",
	})
	reqConflict := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(mutatedBody))
	reqConflict.Header.Set("X-API-Key", "key-alpha")
	reqConflict.Header.Set("Idempotency-Key", "idem-http-1")
	recConflict := httptest.NewRecorder()
	handler.ServeHTTP(recConflict, reqConflict)
	if recConflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on mutated replay, got %d", recConflict.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/transactions/"+submitResp.IntentID, nil)
	getReq.Header.Set("X-API-Key", "key-alpha")
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 GET intent, got %d", getRec.Code)
	}

	otherTenantGet := httptest.NewRequest(http.MethodGet, "/v1/transactions/"+submitResp.IntentID, nil)
	otherTenantGet.Header.Set("X-API-Key", "key-beta")
	otherTenantRec := httptest.NewRecorder()
	handler.ServeHTTP(otherTenantRec, otherTenantGet)
	if otherTenantRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 cross-tenant get, got %d", otherTenantRec.Code)
	}

	cancelReq := httptest.NewRequest(http.MethodPost, "/v1/transactions/"+submitResp.IntentID+"/cancel", strings.NewReader(`{"reason":"no longer needed"}`))
	cancelReq.Header.Set("X-API-Key", "key-alpha")
	cancelRec := httptest.NewRecorder()
	handler.ServeHTTP(cancelRec, cancelReq)
	if cancelRec.Code != http.StatusOK {
		t.Fatalf("expected 200 cancel, got %d: %s", cancelRec.Code, cancelRec.Body.String())
	}
}

func TestAPI_RateLimitEnforced(t *testing.T) {
	t.Parallel()

	handler, from, to := setupTestAPI(t, nil, 1, 1)
	payload, _ := json.Marshal(map[string]any{
		"chain_id":  11155111,
		"from":      from.String(),
		"to":        to.String(),
		"value_wei": "1",
	})

	req1 := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(payload))
	req1.Header.Set("X-API-Key", "key-alpha")
	req1.Header.Set("Idempotency-Key", "rl-1")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected first request 202, got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/v1/transactions", bytes.NewReader(payload))
	req2.Header.Set("X-API-Key", "key-alpha")
	req2.Header.Set("Idempotency-Key", "rl-2")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request 429, got %d", rec2.Code)
	}
}

func TestAPI_NilAuthFailsClosedWithoutPanic(t *testing.T) {
	t.Parallel()

	handler := api.NewHandler(api.HandlerDeps{})
	req := httptest.NewRequest(http.MethodGet, "/v1/transactions/tx-1", nil)
	req.Header.Set("X-API-Key", "any-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when Auth is nil, got %d", rec.Code)
	}
}
