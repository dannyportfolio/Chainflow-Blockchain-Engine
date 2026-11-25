package storetest

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
)

type StoreBundle struct {
	Intents    application.IntentRepository
	Executions application.ExecutionRepository
	Nonces     application.NonceCoordinator
	Wallets    application.WalletRepository
	Outbox     application.OutboxRepository
}

func RunStoreContractTests(t *testing.T, newStore func(t *testing.T) StoreBundle) {
	t.Helper()

	t.Run("intent_idempotency_and_tenant_isolation", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)

		walletAddr, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
		toAddr, _ := domain.ParseAddress("0x2222222222222222222222222222222222222222")
		reqHash1, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		reqHash2, _ := domain.ParseHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

		seedWallet(t, ctx, s.Wallets, walletAddr, 1, now)

		intent := domain.TransactionIntent{
			ID:             "01934a1a-0000-7000-8000-000000000001",
			TenantID:       "tenant-a",
			IdempotencyKey: "idem-001",
			RequestHash:    reqHash1,
			ChainID:        11155111,
			From:           walletAddr,
			To:             &toAddr,
			ValueWei:       big.NewInt(1000),
			MaxFeeCapWei:   big.NewInt(50_000_000_000),
			Status:         domain.IntentQueued,
			MaxAttempts:    5,
			Version:        1,
			ExpiresAt:      now.Add(15 * time.Minute),
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		audit := domain.StateTransitionRecord{
			IntentID:   intent.ID,
			FromStatus: domain.IntentCreated,
			ToStatus:   domain.IntentQueued,
			Actor:      "api",
			OccurredAt: now,
		}
		outbox := application.OutboxEvent{
			EventID:       "01934a1a-0000-7000-8000-000000000010",
			AggregateType: "transaction_intent",
			AggregateID:   intent.ID,
			Subject:       "chainflow.v1.tx.dispatch",
			EventVersion:  1,
			Payload:       []byte(`{"intent_id":"` + intent.ID + `"}`),
			CreatedAt:     now,
			AvailableAt:   now,
		}

		res1, err := s.Intents.CreateIntentWithOutbox(ctx, intent, audit, outbox)
		if err != nil {
			t.Fatalf("initial create failed: %v", err)
		}
		if !res1.Created || res1.Intent.ID != intent.ID {
			t.Fatalf("unexpected initial create result: %+v", res1)
		}

		dup := intent
		dup.ID = "01934a1a-0000-7000-8000-000000000099"
		outboxDup := outbox
		outboxDup.EventID = "01934a1a-0000-7000-8000-000000000098"
		res2, err := s.Intents.CreateIntentWithOutbox(ctx, dup, audit, outboxDup)
		if err != nil {
			t.Fatalf("idempotent replay failed: %v", err)
		}
		if res2.Created || res2.Intent.ID != intent.ID {
			t.Fatalf("expected replay to return original ID %s with Created=false, got %+v", intent.ID, res2)
		}

		conflict := intent
		conflict.ID = "01934a1a-0000-7000-8000-000000000077"
		conflict.RequestHash = reqHash2
		if _, err := s.Intents.CreateIntentWithOutbox(ctx, conflict, audit, outboxDup); !errors.Is(err, application.ErrIdempotencyConflict) {
			t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
		}

		if _, err := s.Intents.GetIntentByID(ctx, "tenant-b", intent.ID); !errors.Is(err, application.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for wrong tenant, got %v", err)
		}
	})

	t.Run("optimistic_locking_and_outbox_lifecycle", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
		walletAddr, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
		reqHash, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		seedWallet(t, ctx, s.Wallets, walletAddr, 1, now)

		intent := domain.TransactionIntent{
			ID:             "01934a1a-0000-7000-8000-000000000002",
			TenantID:       "tenant-a",
			IdempotencyKey: "idem-002",
			RequestHash:    reqHash,
			ChainID:        11155111,
			From:           walletAddr,
			ValueWei:       big.NewInt(0),
			MaxFeeCapWei:   big.NewInt(50_000_000_000),
			Status:         domain.IntentQueued,
			MaxAttempts:    5,
			Version:        1,
			ExpiresAt:      now.Add(15 * time.Minute),
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		outbox := application.OutboxEvent{
			EventID:       "01934a1a-0000-7000-8000-000000000020",
			AggregateType: "transaction_intent",
			AggregateID:   intent.ID,
			Subject:       "chainflow.v1.tx.dispatch",
			EventVersion:  1,
			Payload:       []byte(`{"intent_id":"` + intent.ID + `"}`),
			CreatedAt:     now,
			AvailableAt:   now,
		}
		_, err := s.Intents.CreateIntentWithOutbox(ctx, intent, domain.StateTransitionRecord{
			IntentID: intent.ID, FromStatus: domain.IntentCreated, ToStatus: domain.IntentQueued, Actor: "api", OccurredAt: now,
		}, outbox)
		if err != nil {
			t.Fatalf("create failed: %v", err)
		}

		claimed, err := s.Outbox.ClaimUnpublished(ctx, now, 10*time.Second, 10)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("expected 1 claimed outbox event, got %d, err=%v", len(claimed), err)
		}
		claimed2, err := s.Outbox.ClaimUnpublished(ctx, now.Add(2*time.Second), 10*time.Second, 10)
		if err != nil || len(claimed2) != 0 {
			t.Fatalf("expected 0 claimed inside lease, got %d, err=%v", len(claimed2), err)
		}
		claimed3, err := s.Outbox.ClaimUnpublished(ctx, now.Add(11*time.Second), 10*time.Second, 10)
		if err != nil || len(claimed3) != 1 {
			t.Fatalf("expected 1 claimed after lease expiry, got %d, err=%v", len(claimed3), err)
		}
		if err := s.Outbox.MarkPublished(ctx, []int64{claimed3[0].ID}, now.Add(12*time.Second)); err != nil {
			t.Fatalf("MarkPublished failed: %v", err)
		}
		claimed4, err := s.Outbox.ClaimUnpublished(ctx, now.Add(30*time.Second), 10*time.Second, 10)
		if err != nil || len(claimed4) != 0 {
			t.Fatalf("expected 0 after MarkPublished, got %d", len(claimed4))
		}

		updated := intent
		rec, err := domain.TransitionIntent(&updated, domain.IntentTransitionRequest{
			To: domain.IntentProcessing, Actor: "worker-1", LeaseOwner: "worker-1", LeaseTTL: 30 * time.Second, Now: now,
		})
		if err != nil {
			t.Fatalf("domain transition failed: %v", err)
		}
		if err := s.Intents.TransitionIntentWithOutbox(ctx, 1, updated, rec, nil); err != nil {
			t.Fatalf("TransitionIntentWithOutbox failed: %v", err)
		}
		if err := s.Intents.TransitionIntentWithOutbox(ctx, 1, updated, rec, nil); !errors.Is(err, domain.ErrVersionConflict) {
			t.Fatalf("expected ErrVersionConflict on stale version, got %v", err)
		}

		events, err := s.Intents.ListStateEvents(ctx, intent.ID)
		if err != nil || len(events) != 2 {
			t.Fatalf("expected 2 state audit events, got %d, err=%v", len(events), err)
		}
	})

	t.Run("nonce_reservation_inflight_limit_and_reconciliation", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
		walletAddr, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
		reqHash, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		seedWallet(t, ctx, s.Wallets, walletAddr, 2, now)

		for i, id := range []string{"01934a1a-0000-7000-8000-000000000101", "01934a1a-0000-7000-8000-000000000102", "01934a1a-0000-7000-8000-000000000103"} {
			intent := domain.TransactionIntent{
				ID: id, TenantID: "t", IdempotencyKey: id, RequestHash: reqHash, ChainID: 11155111,
				From: walletAddr, ValueWei: big.NewInt(0), MaxFeeCapWei: big.NewInt(100),
				Status: domain.IntentQueued, MaxAttempts: 5, Version: 1, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
			}
			_, err := s.Intents.CreateIntentWithOutbox(ctx, intent, domain.StateTransitionRecord{
				IntentID: id, FromStatus: domain.IntentCreated, ToStatus: domain.IntentQueued, Actor: "api", OccurredAt: now,
			}, application.OutboxEvent{
				EventID: "01934a1a-0000-7000-8000-00000000020" + string(rune('1'+i)), AggregateType: "transaction_intent", AggregateID: id,
				Subject: "chainflow.v1.tx.dispatch", EventVersion: 1, Payload: []byte(`{}`), CreatedAt: now, AvailableAt: now,
			})
			if err != nil {
				t.Fatalf("seed intent %s: %v", id, err)
			}
		}

		r0, err := s.Nonces.ReserveNextNonce(ctx, 11155111, walletAddr, "01934a1a-0000-7000-8000-000000000101", now)
		if err != nil || r0.Nonce != 0 {
			t.Fatalf("expected nonce 0, got %+v, err=%v", r0, err)
		}
		r1, err := s.Nonces.ReserveNextNonce(ctx, 11155111, walletAddr, "01934a1a-0000-7000-8000-000000000102", now)
		if err != nil || r1.Nonce != 1 {
			t.Fatalf("expected nonce 1, got %+v, err=%v", r1, err)
		}
		if _, err := s.Nonces.ReserveNextNonce(ctx, 11155111, walletAddr, "01934a1a-0000-7000-8000-000000000103", now); !errors.Is(err, application.ErrWalletInflightLimit) {
			t.Fatalf("expected ErrWalletInflightLimit, got %v", err)
		}

		if err := s.Nonces.ConfirmNonce(ctx, 11155111, walletAddr, 0, now); err != nil {
			t.Fatalf("ConfirmNonce failed: %v", err)
		}
		if err := s.Nonces.ReconcilePendingNonce(ctx, 11155111, walletAddr, 5, now); err != nil {
			t.Fatalf("ReconcilePendingNonce failed: %v", err)
		}
		r5, err := s.Nonces.ReserveNextNonce(ctx, 11155111, walletAddr, "01934a1a-0000-7000-8000-000000000103", now)
		if err != nil || r5.Nonce != 5 {
			t.Fatalf("expected reconciled nonce 5, got %+v, err=%v", r5, err)
		}
	})

	t.Run("concurrent_nonce_reservation_no_duplicates", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		now := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
		walletAddr, _ := domain.ParseAddress("0x3333333333333333333333333333333333333333")
		reqHash, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		seedWallet(t, ctx, s.Wallets, walletAddr, 32, now)

		const n = 20
		ids := make([]string, n)
		for i := 0; i < n; i++ {
			id := "01934a1a-0000-7000-8000-0000000010" + string([]byte{'0' + byte(i/10), '0' + byte(i%10)})
			ids[i] = id
			intent := domain.TransactionIntent{
				ID: id, TenantID: "t", IdempotencyKey: id, RequestHash: reqHash, ChainID: 11155111,
				From: walletAddr, ValueWei: big.NewInt(0), MaxFeeCapWei: big.NewInt(100),
				Status: domain.IntentQueued, MaxAttempts: 5, Version: 1, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
			}
			_, err := s.Intents.CreateIntentWithOutbox(ctx, intent, domain.StateTransitionRecord{
				IntentID: id, FromStatus: domain.IntentCreated, ToStatus: domain.IntentQueued, Actor: "api", OccurredAt: now,
			}, application.OutboxEvent{
				EventID:       "01934a1a-0000-7000-8000-0000000020" + string([]byte{'0' + byte(i/10), '0' + byte(i%10)}),
				AggregateType: "transaction_intent", AggregateID: id,
				Subject: "chainflow.v1.tx.dispatch", EventVersion: 1, Payload: []byte(`{}`), CreatedAt: now, AvailableAt: now,
			})
			if err != nil {
				t.Fatalf("seed intent: %v", err)
			}
		}

		var wg sync.WaitGroup
		nonces := make(chan uint64, n)
		errs := make(chan error, n)
		for _, id := range ids {
			id := id
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := s.Nonces.ReserveNextNonce(ctx, 11155111, walletAddr, id, now)
				if err != nil {
					errs <- err
					return
				}
				nonces <- res.Nonce
			}()
		}
		wg.Wait()
		close(nonces)
		close(errs)

		for err := range errs {
			t.Fatalf("unexpected concurrent reserve error: %v", err)
		}
		seen := make(map[uint64]bool, n)
		for nonce := range nonces {
			if seen[nonce] {
				t.Fatalf("duplicate nonce allocated: %d", nonce)
			}
			seen[nonce] = true
		}
		if len(seen) != n {
			t.Fatalf("expected %d unique nonces, got %d", n, len(seen))
		}
	})

	t.Run("GetReceiptByExecutionID", func(t *testing.T) {
		ctx := context.Background()
		s := newStore(t)
		if _, err := s.Executions.GetReceiptByExecutionID(ctx, "01934a1a-0000-7000-8000-000000009999"); !errors.Is(err, application.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for missing receipt, got %v", err)
		}
	})
}

func seedWallet(t *testing.T, ctx context.Context, repo application.WalletRepository, addr domain.Address, maxInflight int, now time.Time) {
	t.Helper()
	err := repo.UpsertWallet(ctx, domain.Wallet{
		ID:                "01934a1a-0000-7000-8000-999999999999",
		ChainID:           11155111,
		Address:           addr,
		Label:             "test-wallet",
		SignerKeyRef:      "dev:test",
		Status:            domain.WalletActive,
		MaxInflightNonces: maxInflight,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, 0)
	if err != nil {
		t.Fatalf("seed wallet failed: %v", err)
	}
}
