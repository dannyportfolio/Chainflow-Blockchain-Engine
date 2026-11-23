package domain_test

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/domain"
)

func newTestIntent(t *testing.T, status domain.IntentStatus) *domain.TransactionIntent {
	t.Helper()

	from, err := domain.ParseAddress("0x1111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("parse from: %v", err)
	}
	to, err := domain.ParseAddress("0x2222222222222222222222222222222222222222")
	if err != nil {
		t.Fatalf("parse to: %v", err)
	}

	now := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	return &domain.TransactionIntent{
		ID:           "01934a1a-0000-7000-8000-000000000001",
		TenantID:     "tenant-alpha",
		ChainID:      11155111,
		From:         from,
		To:           &to,
		ValueWei:     big.NewInt(1_000_000_000_000_000),
		MaxFeeCapWei: big.NewInt(50_000_000_000),
		Status:       status,
		AttemptCount: 0,
		MaxAttempts:  5,
		Version:      1,
		ExpiresAt:    now.Add(15 * time.Minute),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

func TestIntentStateMachine_ValidTransitions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 8, 1, 0, 0, time.UTC)
	txHash, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	outcome := domain.OutcomeSucceeded
	execID := "01934a1b-0000-7000-8000-000000000002"
	retryAt := now.Add(5 * time.Second)

	cases := []struct {
		name      string
		from      domain.IntentStatus
		setup     func(*domain.TransactionIntent)
		req       domain.IntentTransitionRequest
		wantTo    domain.IntentStatus
		wantLease bool
	}{
		{
			name:   "created to queued",
			from:   domain.IntentCreated,
			req:    domain.IntentTransitionRequest{To: domain.IntentQueued, Actor: "api", Now: now},
			wantTo: domain.IntentQueued,
		},
		{
			name:   "created to cancelled",
			from:   domain.IntentCreated,
			req:    domain.IntentTransitionRequest{To: domain.IntentCancelled, Actor: "api", Reason: "caller_cancel", Now: now},
			wantTo: domain.IntentCancelled,
		},
		{
			name:      "queued to processing acquires lease",
			from:      domain.IntentQueued,
			req:       domain.IntentTransitionRequest{To: domain.IntentProcessing, Actor: "worker-1", LeaseOwner: "worker-1", LeaseTTL: 30 * time.Second, Now: now},
			wantTo:    domain.IntentProcessing,
			wantLease: true,
		},
		{
			name: "processing to prepared persists execution and tx_hash",
			from: domain.IntentProcessing,
			setup: func(i *domain.TransactionIntent) {
				i.LeaseOwner = "worker-1"
				exp := now.Add(30 * time.Second)
				i.LeaseExpiresAt = &exp
			},
			req: domain.IntentTransitionRequest{
				To:          domain.IntentPrepared,
				Actor:       "worker-1",
				ExecutionID: execID,
				TxHash:      &txHash,
				LeaseOwner:  "worker-1",
				LeaseTTL:    30 * time.Second,
				Now:         now,
			},
			wantTo:    domain.IntentPrepared,
			wantLease: true,
		},
		{
			name: "prepared to broadcasted clears lease",
			from: domain.IntentPrepared,
			setup: func(i *domain.TransactionIntent) {
				i.ActiveExecutionID = execID
				i.LeaseOwner = "worker-1"
				exp := now.Add(30 * time.Second)
				i.LeaseExpiresAt = &exp
			},
			req:    domain.IntentTransitionRequest{To: domain.IntentBroadcasted, Actor: "worker-1", LeaseOwner: "worker-1", Now: now},
			wantTo: domain.IntentBroadcasted,
		},
		{
			name:   "broadcasted to confirming",
			from:   domain.IntentBroadcasted,
			req:    domain.IntentTransitionRequest{To: domain.IntentConfirming, Actor: "monitor", Now: now},
			wantTo: domain.IntentConfirming,
		},
		{
			name:   "confirming to finalized",
			from:   domain.IntentConfirming,
			req:    domain.IntentTransitionRequest{To: domain.IntentFinalized, Actor: "monitor", Outcome: &outcome, Now: now},
			wantTo: domain.IntentFinalized,
		},
		{
			name:   "confirming to retrying on reorg",
			from:   domain.IntentConfirming,
			req:    domain.IntentTransitionRequest{To: domain.IntentRetrying, Actor: "monitor", Reason: string(domain.RetryReorged), NextRetryAt: &retryAt, Now: now},
			wantTo: domain.IntentRetrying,
		},
		{
			name:   "confirming to broadcasted on shallow reorg",
			from:   domain.IntentConfirming,
			req:    domain.IntentTransitionRequest{To: domain.IntentBroadcasted, Actor: "monitor", Reason: "shallow_reorg_mempool", Now: now},
			wantTo: domain.IntentBroadcasted,
		},
		{
			name:      "retrying to processing",
			from:      domain.IntentRetrying,
			req:       domain.IntentTransitionRequest{To: domain.IntentProcessing, Actor: "worker-2", LeaseOwner: "worker-2", LeaseTTL: 30 * time.Second, Now: now},
			wantTo:    domain.IntentProcessing,
			wantLease: true,
		},
		{
			name:   "broadcasted to replaced",
			from:   domain.IntentBroadcasted,
			req:    domain.IntentTransitionRequest{To: domain.IntentReplaced, Actor: "monitor", Reason: "external_nonce_consumed", Now: now},
			wantTo: domain.IntentReplaced,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			intent := newTestIntent(t, tc.from)
			if tc.setup != nil {
				tc.setup(intent)
			}
			prevVer := intent.Version

			rec, err := domain.TransitionIntent(intent, tc.req)
			if err != nil {
				t.Fatalf("unexpected transition error: %v", err)
			}
			if intent.Status != tc.wantTo {
				t.Fatalf("expected status %s, got %s", tc.wantTo, intent.Status)
			}
			if intent.Version != prevVer+1 {
				t.Fatalf("expected version %d, got %d", prevVer+1, intent.Version)
			}
			if rec.FromStatus != tc.from || rec.ToStatus != tc.wantTo {
				t.Fatalf("audit record mismatch: %+v", rec)
			}
			if tc.wantLease && (intent.LeaseOwner == "" || intent.LeaseExpiresAt == nil) {
				t.Fatal("expected active lease to be populated")
			}
			if !tc.wantLease && (intent.LeaseOwner != "" || intent.LeaseExpiresAt != nil) {
				t.Fatal("expected lease to be cleared")
			}
		})
	}
}

func TestIntentStateMachine_RejectsInvalidAndGuardedTransitions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 8, 1, 0, 0, time.UTC)

	t.Run("rejects outgoing transitions from terminal states", func(t *testing.T) {
		t.Parallel()
		terminals := []domain.IntentStatus{
			domain.IntentFinalized,
			domain.IntentFailed,
			domain.IntentCancelled,
			domain.IntentExpired,
			domain.IntentReplaced,
		}
		for _, term := range terminals {
			intent := newTestIntent(t, term)
			_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
				To:    domain.IntentQueued,
				Actor: "hacker",
				Now:   now,
			})
			if !errors.Is(err, domain.ErrTerminalState) {
				t.Fatalf("terminal %s: expected ErrTerminalState, got %v", term, err)
			}
		}
	})

	t.Run("rejects skipping lifecycle stages", func(t *testing.T) {
		t.Parallel()
		intent := newTestIntent(t, domain.IntentQueued)
		_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
			To:    domain.IntentFinalized,
			Actor: "monitor",
			Now:   now,
		})
		if !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("expected ErrInvalidTransition, got %v", err)
		}
	})

	t.Run("rejects acquiring processing lease when expired", func(t *testing.T) {
		t.Parallel()
		intent := newTestIntent(t, domain.IntentQueued)
		intent.ExpiresAt = now.Add(-1 * time.Second)
		_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
			To:         domain.IntentProcessing,
			Actor:      "worker-1",
			LeaseOwner: "worker-1",
			LeaseTTL:   30 * time.Second,
			Now:        now,
		})
		if !errors.Is(err, domain.ErrIntentExpired) {
			t.Fatalf("expected ErrIntentExpired, got %v", err)
		}
	})

	t.Run("rejects prepared without tx_hash or execution id", func(t *testing.T) {
		t.Parallel()
		intent := newTestIntent(t, domain.IntentProcessing)
		_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
			To:         domain.IntentPrepared,
			Actor:      "worker-1",
			LeaseOwner: "worker-1",
			LeaseTTL:   30 * time.Second,
			Now:        now,
		})
		if !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
	})

	t.Run("rejects retrying past max_attempts", func(t *testing.T) {
		t.Parallel()
		intent := newTestIntent(t, domain.IntentProcessing)
		intent.AttemptCount = 5
		intent.MaxAttempts = 5
		_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
			To:    domain.IntentRetrying,
			Actor: "worker-1",
			Now:   now,
		})
		if !errors.Is(err, domain.ErrMaxAttemptsReached) {
			t.Fatalf("expected ErrMaxAttemptsReached, got %v", err)
		}
	})

	t.Run("rejects finalized without execution outcome", func(t *testing.T) {
		t.Parallel()
		intent := newTestIntent(t, domain.IntentConfirming)
		_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
			To:    domain.IntentFinalized,
			Actor: "monitor",
			Now:   now,
		})
		if !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
	})
	t.Run("rejects hijacking an active lease before expiry", func(t *testing.T) {
		t.Parallel()
		intent := newTestIntent(t, domain.IntentProcessing)
		intent.LeaseOwner = "worker-1"
		exp := now.Add(30 * time.Second)
		intent.LeaseExpiresAt = &exp

		_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
			To:         domain.IntentRetrying,
			Actor:      "worker-2",
			LeaseOwner: "worker-2",
			Now:        now,
		})
		if !errors.Is(err, domain.ErrLeaseConflict) {
			t.Fatalf("expected ErrLeaseConflict, got %v", err)
		}
	})

	t.Run("allows lease sweeper reclaim after lease expiry", func(t *testing.T) {
		t.Parallel()
		intent := newTestIntent(t, domain.IntentProcessing)
		intent.LeaseOwner = "worker-1"
		exp := now.Add(-1 * time.Second)
		intent.LeaseExpiresAt = &exp

		_, err := domain.TransitionIntent(intent, domain.IntentTransitionRequest{
			To:     domain.IntentRetrying,
			Actor:  "lease-sweeper",
			Reason: string(domain.RetryLeaseExpired),
			Now:    now,
		})
		if err != nil {
			t.Fatalf("expected expired lease reclaim to succeed, got %v", err)
		}
	})
}

func TestExecutionStateMachine_Transitions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 8, 1, 0, 0, time.UTC)
	exec := &domain.TransactionExecution{
		ID:            "01934a1b-0000-7000-8000-000000000002",
		IntentID:      "01934a1a-0000-7000-8000-000000000001",
		AttemptNumber: 1,
		Nonce:         42,
		Status:        domain.ExecAllocatingNonce,
	}

	if err := domain.TransitionExecution(exec, domain.ExecSigned, now); err != nil {
		t.Fatalf("allocating -> signed failed: %v", err)
	}
	if !exec.UpdatedAt.Equal(now) {
		t.Fatalf("expected UpdatedAt to be set to %v, got %v", now, exec.UpdatedAt)
	}
	if err := domain.TransitionExecution(exec, domain.ExecBroadcasted, now); err != nil {
		t.Fatalf("signed -> broadcasted failed: %v", err)
	}
	if err := domain.TransitionExecution(exec, domain.ExecConfirming, now); err != nil {
		t.Fatalf("broadcasted -> confirming failed: %v", err)
	}
	if err := domain.TransitionExecution(exec, domain.ExecReorged, now); err != nil {
		t.Fatalf("confirming -> reorged failed: %v", err)
	}
	if err := domain.TransitionExecution(exec, domain.ExecConfirmed, now); !errors.Is(err, domain.ErrTerminalState) {
		t.Fatalf("expected ErrTerminalState from reorged, got %v", err)
	}
}
