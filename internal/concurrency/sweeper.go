package concurrency

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/messaging"
)

type SweeperConfig struct {
	Interval  time.Duration
	BatchSize int
	Now       func() time.Time
	NewID     func() (string, error)
	Logger    *slog.Logger
}

type OrchestratorSweeper struct {
	intents    application.IntentRepository
	executions application.ExecutionRepository
	nonces     application.NonceCoordinator
	cfg        SweeperConfig
	retry      domain.RetryPolicy
}

func NewOrchestratorSweeper(
	intents application.IntentRepository,
	executions application.ExecutionRepository,
	nonces application.NonceCoordinator,
	cfg SweeperConfig,
) *OrchestratorSweeper {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.NewID == nil {
		cfg.NewID = func() (string, error) { return application.NewUUIDv7(cfg.Now()) }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &OrchestratorSweeper{
		intents:    intents,
		executions: executions,
		nonces:     nonces,
		cfg:        cfg,
		retry: domain.RetryPolicy{
			MaxAttempts: 5,
			BaseDelay:   2 * time.Second,
			MaxDelay:    30 * time.Second,
		},
	}
}

func (s *OrchestratorSweeper) Step(ctx context.Context) error {
	var errs []error
	if err := s.sweepStaleLeases(ctx); err != nil {
		errs = append(errs, err)
	}
	if err := s.sweepExpiredIntents(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (s *OrchestratorSweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		if err := s.Step(ctx); err != nil && ctx.Err() == nil {
			s.cfg.Logger.Error("sweeper step failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *OrchestratorSweeper) sweepStaleLeases(ctx context.Context) error {
	now := s.cfg.Now().UTC()
	stale, err := s.intents.ListStaleLeasedIntents(ctx, now, s.cfg.BatchSize)
	if err != nil {
		return err
	}

	for _, intent := range stale {
		expectedVersion := intent.Version
		if intent.AttemptCount < intent.MaxAttempts {
			nextRetry := now.Add(s.retry.NextDelay(intent.AttemptCount, 0.1))
			audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
				To:           domain.IntentRetrying,
				Actor:        "orchestrator:lease-sweeper",
				Reason:       "lease_expired",
				LeaseOwner:   intent.LeaseOwner,
				NextRetryAt:  &nextRetry,
				ErrorCode:    "LEASE_EXPIRED",
				ErrorMessage: "worker lease expired before completion",
				Now:          now,
			})
			if err != nil {
				continue
			}

			eventID, err := s.cfg.NewID()
			if err != nil {
				return err
			}
			payload, err := json.Marshal(messaging.RetryEnvelope{
				EventID:             eventID,
				EventType:           "tx.retry.v1",
				OccurredAt:          now.Format(time.RFC3339Nano),
				IntentID:            intent.ID,
				AttemptNumber:       intent.AttemptCount + 1,
				RetryReason:         "LEASE_EXPIRED",
				PreviousExecutionID: intent.ActiveExecutionID,
				ChainID:             intent.ChainID,
			})
			if err != nil {
				return err
			}
			outbox := &application.OutboxEvent{
				EventID:       eventID,
				AggregateType: "transaction_intent",
				AggregateID:   intent.ID,
				Subject:       messaging.SubjectRetry,
				EventVersion:  1,
				Payload:       payload,
				TraceParent:   intent.TraceID,
				CreatedAt:     now,
				AvailableAt:   nextRetry,
			}
			if err := s.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, outbox); err != nil && !errors.Is(err, domain.ErrVersionConflict) {
				s.cfg.Logger.Warn("reclaim stale lease failed", "intent_id", intent.ID, "error", err)
			}
			continue
		}

		audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
			To:           domain.IntentFailed,
			Actor:        "orchestrator:lease-sweeper",
			Reason:       "lease_expired_max_attempts",
			LeaseOwner:   intent.LeaseOwner,
			ErrorCode:    "MAX_ATTEMPTS_EXHAUSTED",
			ErrorMessage: "worker lease expired after max attempts",
			Now:          now,
		})
		if err != nil {
			continue
		}
		if err := s.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, nil); err != nil && !errors.Is(err, domain.ErrVersionConflict) {
			s.cfg.Logger.Warn("fail stale lease failed", "intent_id", intent.ID, "error", err)
		}
	}
	return nil
}

func (s *OrchestratorSweeper) sweepExpiredIntents(ctx context.Context) error {
	now := s.cfg.Now().UTC()
	expirable, err := s.intents.ListExpirableIntents(ctx, now, s.cfg.BatchSize)
	if err != nil {
		return err
	}

	for _, intent := range expirable {
		expectedVersion := intent.Version
		audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
			To:           domain.IntentExpired,
			Actor:        "orchestrator:expiry-sweeper",
			Reason:       "deadline_exceeded",
			LeaseOwner:   intent.LeaseOwner,
			ErrorCode:    "INTENT_EXPIRED",
			ErrorMessage: "intent expires_at exceeded before broadcast",
			Now:          now,
		})
		if err != nil {
			continue
		}
		if err := s.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, nil); err != nil {
			if !errors.Is(err, domain.ErrVersionConflict) {
				s.cfg.Logger.Warn("expire intent failed", "intent_id", intent.ID, "error", err)
			}
			continue
		}

		if s.executions != nil && s.nonces != nil && intent.ActiveExecutionID != "" {
			execs, err := s.executions.ListExecutionsByIntent(ctx, intent.ID)
			if err == nil {
				for _, ex := range execs {
					if ex.ID == intent.ActiveExecutionID && (ex.Status == domain.ExecAllocatingNonce || ex.Status == domain.ExecSigned) {
						_ = s.nonces.ReleaseUnbroadcastNonce(ctx, ex.ChainID, ex.From, ex.Nonce, now)
						_ = s.executions.UpdateExecutionStatus(ctx, ex.ID, domain.ExecFailed, "INTENT_EXPIRED", "intent expired before broadcast", now)
					}
				}
			}
		}
	}
	return nil
}
