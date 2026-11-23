package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/jackc/pgx/v5"
)

const executionSelectColumns = `
	id::text, intent_id::text, attempt_number, chain_id, from_address, nonce, gas_limit,
	max_priority_fee_wei::text, max_fee_per_gas_wei::text, tx_hash, raw_signed_tx,
	status, rpc_provider_used, broadcasted_at, error_code, error_message, created_at, updated_at
`

func (s *Store) CreateExecution(ctx context.Context, exec domain.TransactionExecution) error {
	var txHashBytes []byte
	if exec.TxHash != nil {
		txHashBytes = exec.TxHash.Bytes()
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO transaction_executions (
			id, intent_id, attempt_number, chain_id, from_address, nonce, gas_limit,
			max_priority_fee_wei, max_fee_per_gas_wei, tx_hash, raw_signed_tx,
			status, rpc_provider_used, broadcasted_at, error_code, error_message, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8::numeric, $9::numeric, $10, $11,
			$12, NULLIF($13, ''), $14, NULLIF($15, ''), NULLIF($16, ''), $17, $18
		)
	`,
		exec.ID, exec.IntentID, exec.AttemptNumber, exec.ChainID, exec.From.Bytes(), exec.Nonce, exec.GasLimit,
		exec.MaxPriorityFeeWei.String(), exec.MaxFeePerGasWei.String(), txHashBytes, exec.RawSignedTx,
		string(exec.Status), exec.RPCProviderUsed, exec.BroadcastedAt, exec.ErrorCode, exec.ErrorMessage,
		exec.CreatedAt, exec.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create execution: %w", err)
	}
	return nil
}

func (s *Store) UpdateExecutionSigned(ctx context.Context, execID string, txHash domain.Hash, rawSignedTx []byte, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE transaction_executions
		SET tx_hash = $2, raw_signed_tx = $3, status = 'SIGNED', updated_at = $4
		WHERE id = $1
	`, execID, txHash.Bytes(), rawSignedTx, now)
	if err != nil {
		return fmt.Errorf("update execution signed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrNotFound
	}
	return nil
}

func (s *Store) UpdateExecutionBroadcasted(ctx context.Context, execID string, provider string, broadcastedAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE transaction_executions
		SET status = 'BROADCASTED', rpc_provider_used = $2, broadcasted_at = $3, updated_at = $3
		WHERE id = $1
	`, execID, provider, broadcastedAt)
	if err != nil {
		return fmt.Errorf("update execution broadcasted: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrNotFound
	}
	return nil
}

func (s *Store) UpdateExecutionStatus(ctx context.Context, execID string, status domain.ExecutionStatus, errCode, errMsg string, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE transaction_executions
		SET status = $2, error_code = NULLIF($3, ''), error_message = NULLIF($4, ''), updated_at = $5
		WHERE id = $1
	`, execID, string(status), errCode, errMsg, now)
	if err != nil {
		return fmt.Errorf("update execution status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrNotFound
	}
	return nil
}

func (s *Store) ListActiveExecutions(ctx context.Context, chainID int64, limit int) ([]domain.TransactionExecution, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+executionSelectColumns+`
		FROM transaction_executions
		WHERE chain_id = $1 AND status IN ('BROADCASTED', 'CONFIRMING')
		ORDER BY broadcasted_at ASC NULLS FIRST, created_at ASC
		LIMIT $2
	`, chainID, limit)
	if err != nil {
		return nil, fmt.Errorf("list active executions: %w", err)
	}
	defer rows.Close()

	return collectExecutions(rows)
}

func (s *Store) ListExecutionsByIntent(ctx context.Context, intentID string) ([]domain.TransactionExecution, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+executionSelectColumns+`
		FROM transaction_executions
		WHERE intent_id = $1
		ORDER BY attempt_number ASC
	`, intentID)
	if err != nil {
		return nil, fmt.Errorf("list executions by intent: %w", err)
	}
	defer rows.Close()

	return collectExecutions(rows)
}

func (s *Store) UpsertReceipt(ctx context.Context, r domain.TransactionReceipt) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO transaction_receipts (
			execution_id, tx_hash, block_number, block_hash, transaction_index,
			gas_used, effective_gas_price_wei, onchain_status, confirmations, reorged, observed_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7::numeric, $8, $9, $10, $11, $12
		)
		ON CONFLICT (execution_id) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash = EXCLUDED.block_hash,
			transaction_index = EXCLUDED.transaction_index,
			gas_used = EXCLUDED.gas_used,
			effective_gas_price_wei = EXCLUDED.effective_gas_price_wei,
			onchain_status = EXCLUDED.onchain_status,
			confirmations = EXCLUDED.confirmations,
			reorged = EXCLUDED.reorged,
			updated_at = EXCLUDED.updated_at
	`,
		r.ExecutionID, r.TxHash.Bytes(), int64(r.BlockNumber), r.BlockHash.Bytes(), int32(r.TransactionIndex),
		int64(r.GasUsed), r.EffectiveGasPrice.String(), int16(r.OnchainStatus), int32(r.Confirmations), r.Reorged,
		r.ObservedAt, r.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert receipt: %w", err)
	}
	return nil
}

func collectExecutions(rows pgx.Rows) ([]domain.TransactionExecution, error) {
	var out []domain.TransactionExecution
	for rows.Next() {
		var e domain.TransactionExecution
		var fromBytes, txHashBytes []byte
		var prioStr, maxFeeStr, statusStr string
		var provider, errCode, errMsg *string

		err := rows.Scan(
			&e.ID, &e.IntentID, &e.AttemptNumber, &e.ChainID, &fromBytes, &e.Nonce, &e.GasLimit,
			&prioStr, &maxFeeStr, &txHashBytes, &e.RawSignedTx,
			&statusStr, &provider, &e.BroadcastedAt, &errCode, &errMsg, &e.CreatedAt, &e.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		if e.From, err = bytesToAddress(fromBytes); err != nil {
			return nil, err
		}
		if len(txHashBytes) > 0 {
			h, err := bytesToHash(txHashBytes)
			if err != nil {
				return nil, err
			}
			e.TxHash = &h
		}
		if e.MaxPriorityFeeWei, err = parseBigInt(prioStr); err != nil {
			return nil, err
		}
		if e.MaxFeePerGasWei, err = parseBigInt(maxFeeStr); err != nil {
			return nil, err
		}
		e.Status = domain.ExecutionStatus(statusStr)
		if provider != nil {
			e.RPCProviderUsed = *provider
		}
		if errCode != nil {
			e.ErrorCode = *errCode
		}
		if errMsg != nil {
			e.ErrorMessage = *errMsg
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
