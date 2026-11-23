package application

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/chainflow/chainflow/internal/domain"
)

type IntentServiceConfig struct {
	Intents             IntentRepository
	Executions          ExecutionRepository
	Wallets             WalletRepository
	ChainID             int64
	DefaultMaxFeeCapWei *big.Int
	DefaultTTL          time.Duration
	DefaultMaxAttempts  int
	Now                 func() time.Time
	NewID               func() (string, error)
}

type IntentService struct {
	intents             IntentRepository
	executions          ExecutionRepository
	wallets             WalletRepository
	chainID             int64
	defaultMaxFeeCapWei *big.Int
	defaultTTL          time.Duration
	defaultMaxAttempts  int
	now                 func() time.Time
	newID               func() (string, error)
}

type SubmitIntentInput struct {
	TenantID         string
	IdempotencyKey   string
	ChainID          int64
	From             domain.Address
	To               *domain.Address
	ValueWei         *big.Int
	Calldata         []byte
	GasLimitOverride *uint64
	MaxFeeCapWei     *big.Int
	Priority         int16
	MaxAttempts      int
	ExpiresAt        *time.Time
	TraceID          string
}

type SubmitIntentResult struct {
	Intent  domain.TransactionIntent
	Created bool
}

type IntentDetail struct {
	Intent     domain.TransactionIntent
	Events     []domain.StateTransitionRecord
	Executions []domain.TransactionExecution
}

type dispatchPayloadV1 struct {
	EventID     string `json:"event_id"`
	EventType   string `json:"event_type"`
	OccurredAt  string `json:"occurred_at"`
	IntentID    string `json:"intent_id"`
	TenantID    string `json:"tenant_id"`
	ChainID     int64  `json:"chain_id"`
	FromAddress string `json:"from_address"`
	Priority    int16  `json:"priority"`
}

func NewIntentService(cfg IntentServiceConfig) *IntentService {
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	idFn := cfg.NewID
	if idFn == nil {
		idFn = func() (string, error) { return NewUUIDv7(nowFn()) }
	}
	defaultTTL := cfg.DefaultTTL
	if defaultTTL <= 0 {
		defaultTTL = 15 * time.Minute
	}
	defaultAttempts := cfg.DefaultMaxAttempts
	if defaultAttempts <= 0 {
		defaultAttempts = 5
	}
	return &IntentService{
		intents:             cfg.Intents,
		executions:          cfg.Executions,
		wallets:             cfg.Wallets,
		chainID:             cfg.ChainID,
		defaultMaxFeeCapWei: cfg.DefaultMaxFeeCapWei,
		defaultTTL:          defaultTTL,
		defaultMaxAttempts:  defaultAttempts,
		now:                 nowFn,
		newID:               idFn,
	}
}

func (s *IntentService) Submit(ctx context.Context, in SubmitIntentInput) (SubmitIntentResult, error) {
	now := s.now().UTC()
	chainID := in.ChainID
	if chainID == 0 {
		chainID = s.chainID
	}
	if chainID != s.chainID {
		return SubmitIntentResult{}, fmt.Errorf("%w: unsupported chain_id %d", domain.ErrInvalidInput, chainID)
	}

	valWei := in.ValueWei
	if valWei == nil {
		valWei = big.NewInt(0)
	}
	maxFeeCap := in.MaxFeeCapWei
	if maxFeeCap == nil && s.defaultMaxFeeCapWei != nil {
		maxFeeCap = new(big.Int).Set(s.defaultMaxFeeCapWei)
	}
	if s.defaultMaxFeeCapWei != nil && maxFeeCap != nil && maxFeeCap.Cmp(s.defaultMaxFeeCapWei) > 0 {
		return SubmitIntentResult{}, fmt.Errorf("%w: requested cap %s exceeds configured limit %s",
			domain.ErrFeeCapExceeded, maxFeeCap.String(), s.defaultMaxFeeCapWei.String())
	}

	maxAttempts := in.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = s.defaultMaxAttempts
	}
	expiresAt := now.Add(s.defaultTTL)
	if in.ExpiresAt != nil {
		expiresAt = in.ExpiresAt.UTC()
	}

	wallet, err := s.wallets.GetWallet(ctx, chainID, in.From)
	if err != nil {
		return SubmitIntentResult{}, err
	}
	if wallet.Status != domain.WalletActive {
		return SubmitIntentResult{}, ErrWalletNotActive
	}

	intentID, err := s.newID()
	if err != nil {
		return SubmitIntentResult{}, fmt.Errorf("generate intent id: %w", err)
	}
	eventID, err := s.newID()
	if err != nil {
		return SubmitIntentResult{}, fmt.Errorf("generate event id: %w", err)
	}

	reqHash := computeRequestHash(chainID, in.From, in.To, valWei, in.Calldata, in.GasLimitOverride, maxFeeCap, in.Priority)

	intent := domain.TransactionIntent{
		ID:               intentID,
		TenantID:         in.TenantID,
		IdempotencyKey:   in.IdempotencyKey,
		RequestHash:      reqHash,
		ChainID:          chainID,
		From:             in.From,
		To:               in.To,
		ValueWei:         new(big.Int).Set(valWei),
		Calldata:         append([]byte(nil), in.Calldata...),
		GasLimitOverride: in.GasLimitOverride,
		MaxFeeCapWei:     maxFeeCap,
		Priority:         in.Priority,
		Status:           domain.IntentCreated,
		MaxAttempts:      maxAttempts,
		Version:          0,
		ExpiresAt:        expiresAt,
		TraceID:          in.TraceID,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := intent.Validate(); err != nil {
		return SubmitIntentResult{}, err
	}

	audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
		To:     domain.IntentQueued,
		Actor:  "api",
		Reason: "intent_accepted",
		Now:    now,
	})
	if err != nil {
		return SubmitIntentResult{}, err
	}

	payloadBytes, err := json.Marshal(dispatchPayloadV1{
		EventID:     eventID,
		EventType:   "tx.dispatch.v1",
		OccurredAt:  now.Format(time.RFC3339Nano),
		IntentID:    intent.ID,
		TenantID:    intent.TenantID,
		ChainID:     intent.ChainID,
		FromAddress: intent.From.String(),
		Priority:    intent.Priority,
	})
	if err != nil {
		return SubmitIntentResult{}, fmt.Errorf("marshal dispatch event: %w", err)
	}

	outbox := OutboxEvent{
		EventID:       eventID,
		AggregateType: "transaction_intent",
		AggregateID:   intent.ID,
		Subject:       "chainflow.v1.tx.dispatch",
		EventVersion:  1,
		Payload:       payloadBytes,
		TraceParent:   in.TraceID,
		CreatedAt:     now,
		AvailableAt:   now,
	}

	res, err := s.intents.CreateIntentWithOutbox(ctx, intent, audit, outbox)
	if err != nil {
		return SubmitIntentResult{}, err
	}
	return SubmitIntentResult(res), nil
}

func (s *IntentService) Get(ctx context.Context, tenantID, intentID string) (IntentDetail, error) {
	intent, err := s.intents.GetIntentByID(ctx, tenantID, intentID)
	if err != nil {
		return IntentDetail{}, err
	}
	events, err := s.intents.ListStateEvents(ctx, intentID)
	if err != nil {
		return IntentDetail{}, err
	}
	var execs []domain.TransactionExecution
	if s.executions != nil {
		execs, err = s.executions.ListExecutionsByIntent(ctx, intentID)
		if err != nil {
			return IntentDetail{}, err
		}
	}
	return IntentDetail{
		Intent:     intent,
		Events:     events,
		Executions: execs,
	}, nil
}

func (s *IntentService) Cancel(ctx context.Context, tenantID, intentID, reason string) (domain.TransactionIntent, error) {
	intent, err := s.intents.GetIntentByID(ctx, tenantID, intentID)
	if err != nil {
		return domain.TransactionIntent{}, err
	}
	if intent.Status == domain.IntentBroadcasted || intent.Status == domain.IntentConfirming {
		return domain.TransactionIntent{}, fmt.Errorf("%w: on-chain cancel replacement requires active executor", domain.ErrInvalidTransition)
	}

	now := s.now().UTC()
	expectedVersion := intent.Version
	audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
		To:     domain.IntentCancelled,
		Actor:  "api:" + tenantID,
		Reason: reason,
		Now:    now,
	})
	if err != nil {
		return domain.TransactionIntent{}, err
	}

	if err := s.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, nil); err != nil {
		return domain.TransactionIntent{}, err
	}
	return intent, nil
}

func computeRequestHash(
	chainID int64,
	from domain.Address,
	to *domain.Address,
	valueWei *big.Int,
	calldata []byte,
	gasLimitOverride *uint64,
	maxFeeCapWei *big.Int,
	priority int16,
) domain.Hash {
	h := sha256.New()
	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(chainID))
	h.Write(buf[:])
	h.Write(from[:])

	if to != nil {
		h.Write([]byte{1})
		h.Write(to[:])
	} else {
		h.Write([]byte{0})
	}

	valBytes := valueWei.Bytes()
	binary.BigEndian.PutUint64(buf[:], uint64(len(valBytes)))
	h.Write(buf[:])
	h.Write(valBytes)

	binary.BigEndian.PutUint64(buf[:], uint64(len(calldata)))
	h.Write(buf[:])
	h.Write(calldata)

	if gasLimitOverride != nil {
		h.Write([]byte{1})
		binary.BigEndian.PutUint64(buf[:], *gasLimitOverride)
		h.Write(buf[:])
	} else {
		h.Write([]byte{0})
	}

	if maxFeeCapWei != nil {
		capBytes := maxFeeCapWei.Bytes()
		binary.BigEndian.PutUint64(buf[:], uint64(len(capBytes)))
		h.Write(buf[:])
		h.Write(capBytes)
	}

	var pbuf [2]byte
	binary.BigEndian.PutUint16(pbuf[:], uint16(priority))
	h.Write(pbuf[:])

	var out domain.Hash
	copy(out[:], h.Sum(nil))
	return out
}

func NewUUIDv7(now time.Time) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	ms := uint64(now.UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
