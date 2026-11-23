package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/jackc/pgx/v5"
)

const intentSelectColumns = `
	id::text, tenant_id, idempotency_key, request_hash, chain_id, from_address, to_address,
	value_wei::text, calldata, gas_limit_override, max_fee_cap_wei::text, priority,
	status, execution_outcome, active_execution_id::text, attempt_count, max_attempts,
	next_retry_at, expires_at, version, lease_owner, lease_expires_at,
	last_error_code, last_error_message, trace_id, created_at, updated_at, finalized_at
`

func (s *Store) CreateIntentWithOutbox(
	ctx context.Context,
	intent domain.TransactionIntent,
	audit domain.StateTransitionRecord,
	outbox application.OutboxEvent,
) (application.IdempotencyCreateResult, error) {
	var result application.IdempotencyCreateResult

	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var toBytes []byte
		if intent.To != nil {
			toBytes = intent.To.Bytes()
		}
		var outcomeStr *string
		if intent.ExecutionOutcome != nil {
			v := string(*intent.ExecutionOutcome)
			outcomeStr = &v
		}
		calldata := intent.Calldata
		if calldata == nil {
			calldata = []byte{}
		}

		var insertedID string
		err := tx.QueryRow(ctx, `
			INSERT INTO transaction_intents (
				id, tenant_id, idempotency_key, request_hash, chain_id, from_address, to_address,
				value_wei, calldata, gas_limit_override, max_fee_cap_wei, priority,
				status, execution_outcome, active_execution_id, attempt_count, max_attempts,
				next_retry_at, expires_at, version, lease_owner, lease_expires_at,
				last_error_code, last_error_message, trace_id, created_at, updated_at, finalized_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8::numeric, $9, $10, $11::numeric, $12,
				$13, $14, NULLIF($15, '')::uuid, $16, $17,
				$18, $19, $20, NULLIF($21, ''), $22,
				NULLIF($23, ''), NULLIF($24, ''), NULLIF($25, ''), $26, $27, $28
			)
			ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
			RETURNING id::text
		`,
			intent.ID, intent.TenantID, intent.IdempotencyKey, intent.RequestHash.Bytes(),
			intent.ChainID, intent.From.Bytes(), toBytes,
			intent.ValueWei.String(), calldata, intent.GasLimitOverride, intent.MaxFeeCapWei.String(), intent.Priority,
			string(intent.Status), outcomeStr, intent.ActiveExecutionID, intent.AttemptCount, intent.MaxAttempts,
			intent.NextRetryAt, intent.ExpiresAt, intent.Version, intent.LeaseOwner, intent.LeaseExpiresAt,
			intent.LastErrorCode, intent.LastErrorMessage, intent.TraceID, intent.CreatedAt, intent.UpdatedAt, intent.FinalizedAt,
		).Scan(&insertedID)

		if errors.Is(err, pgx.ErrNoRows) {
			existing, scanErr := scanIntent(tx.QueryRow(ctx,
				`SELECT `+intentSelectColumns+` FROM transaction_intents WHERE tenant_id = $1 AND idempotency_key = $2`,
				intent.TenantID, intent.IdempotencyKey,
			))
			if scanErr != nil {
				return fmt.Errorf("fetch existing idempotent intent: %w", scanErr)
			}
			if existing.RequestHash != intent.RequestHash {
				return application.ErrIdempotencyConflict
			}
			result = application.IdempotencyCreateResult{Intent: existing, Created: false}
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert intent: %w", err)
		}

		if err := insertStateEvent(ctx, tx, audit); err != nil {
			return err
		}
		if err := insertOutboxEvent(ctx, tx, outbox); err != nil {
			return err
		}

		result = application.IdempotencyCreateResult{Intent: intent, Created: true}
		return nil
	})
	return result, err
}

func (s *Store) GetIntentByID(ctx context.Context, tenantID, intentID string) (domain.TransactionIntent, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+intentSelectColumns+` FROM transaction_intents WHERE id = $1 AND tenant_id = $2`,
		intentID, tenantID,
	)
	intent, err := scanIntent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.TransactionIntent{}, application.ErrNotFound
	}
	if err != nil {
		return domain.TransactionIntent{}, fmt.Errorf("get intent: %w", err)
	}
	return intent, nil
}

func (s *Store) GetIntentByIDForSystem(ctx context.Context, intentID string) (domain.TransactionIntent, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+intentSelectColumns+` FROM transaction_intents WHERE id = $1`,
		intentID,
	)
	intent, err := scanIntent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.TransactionIntent{}, application.ErrNotFound
	}
	if err != nil {
		return domain.TransactionIntent{}, fmt.Errorf("get intent for system: %w", err)
	}
	return intent, nil
}

func (s *Store) TransitionIntentWithOutbox(
	ctx context.Context,
	expectedVersion int64,
	updated domain.TransactionIntent,
	audit domain.StateTransitionRecord,
	outbox *application.OutboxEvent,
) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		var outcomeStr *string
		if updated.ExecutionOutcome != nil {
			v := string(*updated.ExecutionOutcome)
			outcomeStr = &v
		}

		tag, err := tx.Exec(ctx, `
			UPDATE transaction_intents SET
				status = $3,
				execution_outcome = $4,
				active_execution_id = NULLIF($5, '')::uuid,
				attempt_count = $6,
				next_retry_at = $7,
				version = $8,
				lease_owner = NULLIF($9, ''),
				lease_expires_at = $10,
				last_error_code = NULLIF($11, ''),
				last_error_message = NULLIF($12, ''),
				updated_at = $13,
				finalized_at = $14
			WHERE id = $1 AND version = $2
		`,
			updated.ID, expectedVersion,
			string(updated.Status), outcomeStr, updated.ActiveExecutionID,
			updated.AttemptCount, updated.NextRetryAt, updated.Version,
			updated.LeaseOwner, updated.LeaseExpiresAt,
			updated.LastErrorCode, updated.LastErrorMessage,
			updated.UpdatedAt, updated.FinalizedAt,
		)
		if err != nil {
			return fmt.Errorf("update intent transition: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrVersionConflict
		}

		if err := insertStateEvent(ctx, tx, audit); err != nil {
			return err
		}
		if outbox != nil {
			if err := insertOutboxEvent(ctx, tx, *outbox); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) ListStaleLeasedIntents(ctx context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error) {
	return s.queryIntentList(ctx, `
		SELECT `+intentSelectColumns+`
		FROM transaction_intents
		WHERE status IN ('PROCESSING', 'PREPARED') AND lease_expires_at IS NOT NULL AND lease_expires_at <= $1
		ORDER BY lease_expires_at ASC
		LIMIT $2
	`, now, limit)
}

func (s *Store) ListDueRetries(ctx context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error) {
	return s.queryIntentList(ctx, `
		SELECT `+intentSelectColumns+`
		FROM transaction_intents
		WHERE status = 'RETRYING' AND (next_retry_at IS NULL OR next_retry_at <= $1)
		ORDER BY next_retry_at ASC NULLS FIRST
		LIMIT $2
	`, now, limit)
}

func (s *Store) ListExpirableIntents(ctx context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error) {
	return s.queryIntentList(ctx, `
		SELECT `+intentSelectColumns+`
		FROM transaction_intents
		WHERE status IN ('CREATED', 'QUEUED', 'PROCESSING', 'PREPARED', 'RETRYING') AND expires_at <= $1
		ORDER BY expires_at ASC
		LIMIT $2
	`, now, limit)
}

func (s *Store) ListStateEvents(ctx context.Context, intentID string) ([]domain.StateTransitionRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT intent_id::text, COALESCE(execution_id::text, ''), COALESCE(from_status, ''), to_status, actor, COALESCE(reason, ''), created_at
		FROM transaction_state_events
		WHERE intent_id = $1
		ORDER BY id ASC
	`, intentID)
	if err != nil {
		return nil, fmt.Errorf("list state events: %w", err)
	}
	defer rows.Close()

	var out []domain.StateTransitionRecord
	for rows.Next() {
		var r domain.StateTransitionRecord
		var fromStr, toStr string
		if err := rows.Scan(&r.IntentID, &r.ExecutionID, &fromStr, &toStr, &r.Actor, &r.Reason, &r.OccurredAt); err != nil {
			return nil, fmt.Errorf("scan state event: %w", err)
		}
		r.FromStatus = domain.IntentStatus(fromStr)
		r.ToStatus = domain.IntentStatus(toStr)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) queryIntentList(ctx context.Context, sql string, now time.Time, limit int) ([]domain.TransactionIntent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, sql, now, limit)
	if err != nil {
		return nil, fmt.Errorf("query intent list: %w", err)
	}
	defer rows.Close()

	var out []domain.TransactionIntent
	for rows.Next() {
		intent, err := scanIntent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, rows.Err()
}

func scanIntent(row pgx.Row) (domain.TransactionIntent, error) {
	var i domain.TransactionIntent
	var reqHashBytes, fromBytes, toBytes []byte
	var valueStr, maxFeeStr, statusStr string
	var outcomeStr, activeExecID, leaseOwner, errCode, errMsg, traceID *string

	err := row.Scan(
		&i.ID, &i.TenantID, &i.IdempotencyKey, &reqHashBytes, &i.ChainID, &fromBytes, &toBytes,
		&valueStr, &i.Calldata, &i.GasLimitOverride, &maxFeeStr, &i.Priority,
		&statusStr, &outcomeStr, &activeExecID, &i.AttemptCount, &i.MaxAttempts,
		&i.NextRetryAt, &i.ExpiresAt, &i.Version, &leaseOwner, &i.LeaseExpiresAt,
		&errCode, &errMsg, &traceID, &i.CreatedAt, &i.UpdatedAt, &i.FinalizedAt,
	)
	if err != nil {
		return domain.TransactionIntent{}, err
	}

	if i.RequestHash, err = bytesToHash(reqHashBytes); err != nil {
		return domain.TransactionIntent{}, err
	}
	if i.From, err = bytesToAddress(fromBytes); err != nil {
		return domain.TransactionIntent{}, err
	}
	if len(toBytes) > 0 {
		addr, err := bytesToAddress(toBytes)
		if err != nil {
			return domain.TransactionIntent{}, err
		}
		i.To = &addr
	}
	if i.ValueWei, err = parseBigInt(valueStr); err != nil {
		return domain.TransactionIntent{}, err
	}
	if i.MaxFeeCapWei, err = parseBigInt(maxFeeStr); err != nil {
		return domain.TransactionIntent{}, err
	}

	i.Status = domain.IntentStatus(statusStr)
	if outcomeStr != nil {
		o := domain.ExecutionOutcome(*outcomeStr)
		i.ExecutionOutcome = &o
	}
	if activeExecID != nil {
		i.ActiveExecutionID = *activeExecID
	}
	if leaseOwner != nil {
		i.LeaseOwner = *leaseOwner
	}
	if errCode != nil {
		i.LastErrorCode = *errCode
	}
	if errMsg != nil {
		i.LastErrorMessage = *errMsg
	}
	if traceID != nil {
		i.TraceID = *traceID
	}
	return i, nil
}

func insertStateEvent(ctx context.Context, tx pgx.Tx, r domain.StateTransitionRecord) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO transaction_state_events (intent_id, execution_id, from_status, to_status, actor, reason, created_at)
		VALUES ($1, NULLIF($2, '')::uuid, NULLIF($3, ''), $4, $5, NULLIF($6, ''), $7)
	`, r.IntentID, r.ExecutionID, string(r.FromStatus), string(r.ToStatus), r.Actor, r.Reason, r.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert state event: %w", err)
	}
	return nil
}

func insertOutboxEvent(ctx context.Context, tx pgx.Tx, ev application.OutboxEvent) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, aggregate_type, aggregate_id, subject, event_version, payload, trace_parent, created_at, available_at
		) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9)
	`, ev.EventID, ev.AggregateType, ev.AggregateID, ev.Subject, ev.EventVersion, ev.Payload, ev.TraceParent, ev.CreatedAt, ev.AvailableAt)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}
