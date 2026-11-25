package application_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/persistence/memory"
)

type stubChainObserver struct {
	head        uint64
	latestNonce uint64
	receipts    map[domain.Hash]blockchain.Receipt
	txs         map[domain.Hash]blockchain.ChainTransaction
}

func (s *stubChainObserver) BlockNumber(_ context.Context) (uint64, error) {
	return s.head, nil
}

func (s *stubChainObserver) LatestNonceAt(_ context.Context, _ domain.Address) (uint64, error) {
	return s.latestNonce, nil
}

func (s *stubChainObserver) TransactionReceipt(_ context.Context, h domain.Hash) (blockchain.Receipt, error) {
	if r, ok := s.receipts[h]; ok {
		return r, nil
	}
	return blockchain.Receipt{}, blockchain.ErrReceiptNotFound
}

func (s *stubChainObserver) TransactionByHash(_ context.Context, h domain.Hash) (blockchain.ChainTransaction, error) {
	if tx, ok := s.txs[h]; ok {
		return tx, nil
	}
	return blockchain.ChainTransaction{}, blockchain.ErrTransactionNotFound
}

type stubVerifier struct {
	ver blockchain.BlockVerification
}

func (s *stubVerifier) VerifyBlockHash(_ context.Context, _ uint64, _ domain.Hash) (blockchain.BlockVerification, error) {
	return s.ver, nil
}

func seedBroadcastedIntentAndExec(t *testing.T, store *memory.Store, now time.Time) (domain.TransactionIntent, domain.TransactionExecution, domain.Hash) {
	t.Helper()
	ctx := context.Background()

	from, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
	to, _ := domain.ParseAddress("0x2222222222222222222222222222222222222222")
	reqHash, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	txHash, _ := domain.ParseHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	_ = store.UpsertWallet(ctx, domain.Wallet{
		ID:                "01934a1a-0000-7000-8000-000000000001",
		ChainID:           11155111,
		Address:           from,
		Label:             "w1",
		SignerKeyRef:      "dev:1",
		Status:            domain.WalletActive,
		MaxInflightNonces: 1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, 0)

	intent := domain.TransactionIntent{
		ID:             "01934a1a-0000-7000-8000-000000000010",
		TenantID:       "tenant-1",
		IdempotencyKey: "idem-1",
		RequestHash:    reqHash,
		ChainID:        11155111,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(100),
		MaxFeeCapWei:   big.NewInt(100_000_000_000),
		MaxAttempts:    5,
		AttemptCount:   1,
		Status:         domain.IntentBroadcasted,
		Version:        1,
		ExpiresAt:      now.Add(10 * time.Minute),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	audit := domain.StateTransitionRecord{
		IntentID: intent.ID, ToStatus: domain.IntentBroadcasted, Actor: "test", Reason: "seed", OccurredAt: now,
	}
	outbox := application.OutboxEvent{
		EventID: "01934a1a-0000-7000-8000-000000000012", AggregateType: "transaction_intent",
		AggregateID: intent.ID, Subject: "chainflow.tx.dispatch", EventVersion: 1,
		Payload: []byte(`{}`), CreatedAt: now, AvailableAt: now,
	}
	if _, err := store.CreateIntentWithOutbox(ctx, intent, audit, outbox); err != nil {
		t.Fatalf("seed intent failed: %v", err)
	}
	res, err := store.ReserveNextNonce(ctx, 11155111, from, intent.ID, now)
	if err != nil {
		t.Fatalf("reserve nonce failed: %v", err)
	}

	exec := domain.TransactionExecution{
		ID:                "01934a1a-0000-7000-8000-000000000020",
		IntentID:          intent.ID,
		AttemptNumber:     1,
		ChainID:           11155111,
		From:              from,
		Nonce:             res.Nonce,
		GasLimit:          21000,
		MaxPriorityFeeWei: big.NewInt(1_000_000_000),
		MaxFeePerGasWei:   big.NewInt(10_000_000_000),
		TxHash:            &txHash,
		Status:            domain.ExecBroadcasted,
		BroadcastedAt:     &now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := store.CreateExecution(ctx, exec); err != nil {
		t.Fatalf("create execution failed: %v", err)
	}
	return intent, exec, txHash
}

func TestConfirmationMonitor_LifecycleTransitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	blockHash, _ := domain.ParseHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

	t.Run("broadcasted -> confirming -> finalized (reverted on-chain)", func(t *testing.T) {
		store := memory.NewStore()
		intent, _, txHash := seedBroadcastedIntentAndExec(t, store, now)
		chain := &stubChainObserver{
			head: 100,
			receipts: map[domain.Hash]blockchain.Receipt{
				txHash: {TxHash: txHash, BlockNumber: 100, BlockHash: blockHash, GasUsed: 21000, Status: 0},
			},
		}
		verifier := &stubVerifier{ver: blockchain.BlockVerification{BlockNumber: 100, CanonicalHash: blockHash, Matches: true}}
		mon := application.NewConfirmationMonitor(store, store, store, chain, verifier, application.ConfirmationMonitorConfig{
			ChainID: 11155111, RequiredConfirmations: 3, Now: func() time.Time { return now },
		})

		if err := mon.RunOnce(ctx); err != nil {
			t.Fatalf("run 1 failed: %v", err)
		}
		got, _ := store.GetIntentByIDForSystem(ctx, intent.ID)
		if got.Status != domain.IntentConfirming {
			t.Fatalf("expected CONFIRMING, got %s", got.Status)
		}

		chain.head = 102 // 102 - 100 + 1 = 3 confirmations
		if err := mon.RunOnce(ctx); err != nil {
			t.Fatalf("run 2 failed: %v", err)
		}
		got, _ = store.GetIntentByIDForSystem(ctx, intent.ID)
		if got.Status != domain.IntentFinalized || got.ExecutionOutcome == nil || *got.ExecutionOutcome != domain.OutcomeReverted {
			t.Fatalf("expected FINALIZED(REVERTED), got %s (%v)", got.Status, got.ExecutionOutcome)
		}
	})

	t.Run("stale pending triggers RBF retry with outbox event", func(t *testing.T) {
		store := memory.NewStore()
		intent, _, txHash := seedBroadcastedIntentAndExec(t, store, now)
		chain := &stubChainObserver{
			head: 100,
			txs:  map[domain.Hash]blockchain.ChainTransaction{txHash: {Hash: txHash, Pending: true}},
		}
		later := now.Add(3 * time.Minute)
		mon := application.NewConfirmationMonitor(store, store, store, chain, nil, application.ConfirmationMonitorConfig{
			ChainID: 11155111, StalePendingTTL: 2 * time.Minute, Now: func() time.Time { return later },
		})

		if err := mon.RunOnce(ctx); err != nil {
			t.Fatalf("run failed: %v", err)
		}
		got, _ := store.GetIntentByIDForSystem(ctx, intent.ID)
		if got.Status != domain.IntentRetrying {
			t.Fatalf("expected RETRYING, got %s", got.Status)
		}
	})

	t.Run("external nonce consumption marks intent REPLACED", func(t *testing.T) {
		store := memory.NewStore()
		intent, _, _ := seedBroadcastedIntentAndExec(t, store, now)
		chain := &stubChainObserver{head: 100, latestNonce: 1}
		mon := application.NewConfirmationMonitor(store, store, store, chain, nil, application.ConfirmationMonitorConfig{
			ChainID: 11155111, Now: func() time.Time { return now },
		})

		if err := mon.RunOnce(ctx); err != nil {
			t.Fatalf("run failed: %v", err)
		}
		got, _ := store.GetIntentByIDForSystem(ctx, intent.ID)
		if got.Status != domain.IntentReplaced {
			t.Fatalf("expected REPLACED, got %s", got.Status)
		}
	})

	t.Run("reorg evicts tx -> RETRYING", func(t *testing.T) {
		store := memory.NewStore()
		intent, _, txHash := seedBroadcastedIntentAndExec(t, store, now)
		chain := &stubChainObserver{
			head: 100,
			receipts: map[domain.Hash]blockchain.Receipt{
				txHash: {TxHash: txHash, BlockNumber: 100, BlockHash: blockHash, GasUsed: 21000, Status: 1},
			},
		}
		verifier := &stubVerifier{ver: blockchain.BlockVerification{BlockNumber: 100, CanonicalHash: blockHash, Matches: true}}
		mon := application.NewConfirmationMonitor(store, store, store, chain, verifier, application.ConfirmationMonitorConfig{
			ChainID: 11155111, RequiredConfirmations: 3, Now: func() time.Time { return now },
		})

		if err := mon.RunOnce(ctx); err != nil {
			t.Fatalf("run 1 failed: %v", err)
		}
		// Simulate reorg: receipt gone, verifier sees different block hash
		delete(chain.receipts, txHash)
		verifier.ver = blockchain.BlockVerification{BlockNumber: 100, Reorged: true}
		if err := mon.RunOnce(ctx); err != nil {
			t.Fatalf("run 2 failed: %v", err)
		}
		got, _ := store.GetIntentByIDForSystem(ctx, intent.ID)
		if got.Status != domain.IntentRetrying {
			t.Fatalf("expected RETRYING after reorg eviction, got %s", got.Status)
		}
	})
}
