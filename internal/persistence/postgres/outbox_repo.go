package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/chainflow/chainflow/internal/application"
)

func (s *Store) ClaimUnpublished(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) ([]application.OutboxEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	lockedUntil := now.Add(leaseDuration)

	rows, err := s.pool.Query(ctx, `
		WITH claimable AS (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
			  AND available_at <= $1
			  AND (locked_until IS NULL OR locked_until <= $1)
			ORDER BY available_at ASC, id ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events o
		SET locked_until = $2,
			publish_attempts = o.publish_attempts + 1
		FROM claimable c
		WHERE o.id = c.id
		RETURNING o.id, o.event_id::text, o.aggregate_type, o.aggregate_id::text, o.subject,
			o.event_version, o.payload, COALESCE(o.trace_parent, ''), o.created_at, o.available_at,
			o.locked_until, o.published_at, o.publish_attempts, COALESCE(o.last_error, '')
	`, now, lockedUntil, limit)
	if err != nil {
		return nil, fmt.Errorf("claim unpublished outbox events: %w", err)
	}
	defer rows.Close()

	var out []application.OutboxEvent
	for rows.Next() {
		var ev application.OutboxEvent
		if err := rows.Scan(
			&ev.ID, &ev.EventID, &ev.AggregateType, &ev.AggregateID, &ev.Subject,
			&ev.EventVersion, &ev.Payload, &ev.TraceParent, &ev.CreatedAt, &ev.AvailableAt,
			&ev.LockedUntil, &ev.PublishedAt, &ev.PublishAttempts, &ev.LastError,
		); err != nil {
			return nil, fmt.Errorf("scan claimed outbox event: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, ids []int64, publishedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = $2, locked_until = NULL
		WHERE id = ANY($1) AND published_at IS NULL
	`, ids, publishedAt)
	if err != nil {
		return fmt.Errorf("mark outbox events published: %w", err)
	}
	return nil
}

func (s *Store) MarkPublishFailed(ctx context.Context, id int64, retryAt time.Time, errMsg string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET available_at = $2, locked_until = NULL, last_error = $3
		WHERE id = $1 AND published_at IS NULL
	`, id, retryAt, errMsg)
	if err != nil {
		return fmt.Errorf("mark outbox publish failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrNotFound
	}
	return nil
}

func (s *Store) DeletePublishedBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 5000
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM outbox_events
		WHERE id IN (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NOT NULL AND published_at < $1
			ORDER BY id ASC
			LIMIT $2
		)
	`, before, limit)
	if err != nil {
		return 0, fmt.Errorf("delete published outbox events: %w", err)
	}
	return tag.RowsAffected(), nil
}
