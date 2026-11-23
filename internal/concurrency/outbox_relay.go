package concurrency

import (
	"context"
	"log/slog"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/messaging"
)

type OutboxRelayConfig struct {
	PollInterval    time.Duration
	ClaimLease      time.Duration
	BatchSize       int
	PruneEvery      time.Duration
	RetentionPeriod time.Duration
	Now             func() time.Time
	Logger          *slog.Logger
}

type OutboxRelay struct {
	repo      application.OutboxRepository
	publisher messaging.EventPublisher
	cfg       OutboxRelayConfig
	retry     domain.RetryPolicy
	lastPrune time.Time
}

func NewOutboxRelay(repo application.OutboxRepository, publisher messaging.EventPublisher, cfg OutboxRelayConfig) *OutboxRelay {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.ClaimLease <= 0 {
		cfg.ClaimLease = 15 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PruneEvery <= 0 {
		cfg.PruneEvery = 5 * time.Minute
	}
	if cfg.RetentionPeriod <= 0 {
		cfg.RetentionPeriod = 24 * time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &OutboxRelay{
		repo:      repo,
		publisher: publisher,
		cfg:       cfg,
		retry: domain.RetryPolicy{
			MaxAttempts: 10,
			BaseDelay:   500 * time.Millisecond,
			MaxDelay:    30 * time.Second,
		},
	}
}

func (r *OutboxRelay) Step(ctx context.Context) (int, error) {
	now := r.cfg.Now().UTC()
	events, err := r.repo.ClaimUnpublished(ctx, now, r.cfg.ClaimLease, r.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	if len(events) > 0 {
		publishedIDs := make([]int64, 0, len(events))
		for _, ev := range events {
			if err := r.publisher.Publish(ctx, ev); err != nil {
				delay := r.retry.NextDelay(ev.PublishAttempts, 0.1)
				retryAt := r.cfg.Now().UTC().Add(delay)
				if markErr := r.repo.MarkPublishFailed(ctx, ev.ID, retryAt, err.Error()); markErr != nil {
					r.cfg.Logger.Error("mark outbox publish failed", "outbox_id", ev.ID, "error", markErr)
				}
				continue
			}
			publishedIDs = append(publishedIDs, ev.ID)
		}
		if len(publishedIDs) > 0 {
			if err := r.repo.MarkPublished(ctx, publishedIDs, r.cfg.Now().UTC()); err != nil {
				return len(publishedIDs), err
			}
		}
		return len(publishedIDs), nil
	}

	if r.lastPrune.IsZero() || now.Sub(r.lastPrune) >= r.cfg.PruneEvery {
		if _, err := r.repo.DeletePublishedBefore(ctx, now.Add(-r.cfg.RetentionPeriod), 1000); err != nil {
			r.cfg.Logger.Warn("prune published outbox events failed", "error", err)
		} else {
			r.lastPrune = now
		}
	}
	return 0, nil
}

func (r *OutboxRelay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if _, err := r.Step(ctx); err != nil && ctx.Err() == nil {
			r.cfg.Logger.Error("outbox relay step failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
