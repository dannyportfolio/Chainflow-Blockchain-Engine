package application_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/persistence/memory"
)

func setupIntentService(t *testing.T) (*application.IntentService, *memory.Store, domain.Address, domain.Address) {
	t.Helper()

	store := memory.NewStore()
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	from, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
	to, _ := domain.ParseAddress("0x2222222222222222222222222222222222222222")

	err := store.UpsertWallet(context.Background(), domain.Wallet{
		ID:                "01934a1a-0000-7000-8000-999999999999",
		ChainID:           11155111,
		Address:           from,
		Label:             "sepolia-hot-1",
		SignerKeyRef:      "dev:sepolia-1",
		Status:            domain.WalletActive,
		MaxInflightNonces: 1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, 0)
	if err != nil {
		t.Fatalf("seed wallet: %v", err)
	}

	svc := application.NewIntentService(application.IntentServiceConfig{
		Intents:             store,
		Wallets:             store,
		ChainID:             11155111,
		DefaultMaxFeeCapWei: big.NewInt(100_000_000_000),
		DefaultTTL:          15 * time.Minute,
		DefaultMaxAttempts:  5,
		Now:                 func() time.Time { return now },
	})
	return svc, store, from, to
}

func TestIntentService_SubmitAndIdempotency(t *testing.T) {
	t.Parallel()

	svc, store, from, to := setupIntentService(t)
	ctx := context.Background()

	in := application.SubmitIntentInput{
		TenantID:       "tenant-a",
		IdempotencyKey: "req-001",
		ChainID:        11155111,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(1000),
		Calldata:       []byte{0xde, 0xad, 0xbe, 0xef},
		MaxFeeCapWei:   big.NewInt(50_000_000_000),
		Priority:       1,
		TraceID:        "trace-123",
	}

	res1, err := svc.Submit(ctx, in)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if !res1.Created {
		t.Fatal("expected Created=true on initial submit")
	}
	if res1.Intent.Status != domain.IntentQueued {
		t.Fatalf("expected QUEUED status, got %s", res1.Intent.Status)
	}

	claimed, err := store.ClaimUnpublished(ctx, time.Date(2026, 11, 21, 10, 0, 1, 0, time.UTC), 10*time.Second, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("expected 1 outbox event, got %d, err=%v", len(claimed), err)
	}
	if claimed[0].Subject != "chainflow.v1.tx.dispatch" {
		t.Fatalf("unexpected outbox subject: %s", claimed[0].Subject)
	}

	res2, err := svc.Submit(ctx, in)
	if err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}
	if res2.Created || res2.Intent.ID != res1.Intent.ID {
		t.Fatalf("expected replay to return original ID %s with Created=false, got %+v", res1.Intent.ID, res2)
	}

	mutated := in
	mutated.ValueWei = big.NewInt(2000)
	if _, err := svc.Submit(ctx, mutated); !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict on mutated replay, got %v", err)
	}
}

func TestIntentService_ValidationAndWalletGuards(t *testing.T) {
	t.Parallel()

	svc, store, from, to := setupIntentService(t)
	ctx := context.Background()

	_, err := svc.Submit(ctx, application.SubmitIntentInput{
		TenantID:       "tenant-a",
		IdempotencyKey: "bad-chain",
		ChainID:        1,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(0),
	})
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for wrong chain_id, got %v", err)
	}

	_, err = svc.Submit(ctx, application.SubmitIntentInput{
		TenantID:       "tenant-a",
		IdempotencyKey: "too-high-cap",
		ChainID:        11155111,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(0),
		MaxFeeCapWei:   big.NewInt(500_000_000_000),
	})
	if !errors.Is(err, domain.ErrFeeCapExceeded) {
		t.Fatalf("expected ErrFeeCapExceeded, got %v", err)
	}

	unknownWallet, _ := domain.ParseAddress("0x9999999999999999999999999999999999999999")
	_, err = svc.Submit(ctx, application.SubmitIntentInput{
		TenantID:       "tenant-a",
		IdempotencyKey: "missing-wallet",
		ChainID:        11155111,
		From:           unknownWallet,
		To:             &to,
		ValueWei:       big.NewInt(0),
	})
	if !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unregistered wallet, got %v", err)
	}

	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	_ = store.UpsertWallet(ctx, domain.Wallet{
		ID:                "01934a1a-0000-7000-8000-999999999999",
		ChainID:           11155111,
		Address:           from,
		Label:             "paused",
		SignerKeyRef:      "dev:sepolia-1",
		Status:            domain.WalletPaused,
		MaxInflightNonces: 1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, 0)

	_, err = svc.Submit(ctx, application.SubmitIntentInput{
		TenantID:       "tenant-a",
		IdempotencyKey: "paused-wallet",
		ChainID:        11155111,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(0),
	})
	if !errors.Is(err, application.ErrWalletNotActive) {
		t.Fatalf("expected ErrWalletNotActive, got %v", err)
	}
}

func TestIntentService_GetAndCancel(t *testing.T) {
	t.Parallel()

	svc, _, from, to := setupIntentService(t)
	ctx := context.Background()

	res, err := svc.Submit(ctx, application.SubmitIntentInput{
		TenantID:       "tenant-a",
		IdempotencyKey: "cancel-me",
		ChainID:        11155111,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(100),
	})
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	detail, err := svc.Get(ctx, "tenant-a", res.Intent.ID)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(detail.Events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(detail.Events))
	}

	if _, err := svc.Get(ctx, "tenant-b", res.Intent.ID); !errors.Is(err, application.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for cross-tenant get, got %v", err)
	}

	cancelled, err := svc.Cancel(ctx, "tenant-a", res.Intent.ID, "caller_requested")
	if err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	if cancelled.Status != domain.IntentCancelled {
		t.Fatalf("expected CANCELLED, got %s", cancelled.Status)
	}

	if _, err := svc.Cancel(ctx, "tenant-a", res.Intent.ID, "again"); !errors.Is(err, domain.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState on second cancel, got %v", err)
	}
}
