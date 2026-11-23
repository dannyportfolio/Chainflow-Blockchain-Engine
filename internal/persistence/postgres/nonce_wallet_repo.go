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

func (s *Store) UpsertWallet(ctx context.Context, w domain.Wallet, initialNonce uint64) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO wallets (id, chain_id, address, label, signer_key_ref, status, max_inflight_nonces, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (chain_id, address) DO UPDATE SET
				label = EXCLUDED.label,
				signer_key_ref = EXCLUDED.signer_key_ref,
				status = EXCLUDED.status,
				max_inflight_nonces = EXCLUDED.max_inflight_nonces,
				updated_at = EXCLUDED.updated_at
		`, w.ID, w.ChainID, w.Address.Bytes(), w.Label, w.SignerKeyRef, string(w.Status), w.MaxInflightNonces, w.CreatedAt, w.UpdatedAt)
		if err != nil {
			return fmt.Errorf("upsert wallet: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO wallet_nonces (chain_id, wallet_address, next_nonce, updated_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (chain_id, wallet_address) DO NOTHING
		`, w.ChainID, w.Address.Bytes(), int64(initialNonce), w.UpdatedAt)
		if err != nil {
			return fmt.Errorf("init wallet_nonces: %w", err)
		}
		return nil
	})
}

func (s *Store) GetWallet(ctx context.Context, chainID int64, address domain.Address) (domain.Wallet, error) {
	var w domain.Wallet
	var addrBytes []byte
	var statusStr string

	err := s.pool.QueryRow(ctx, `
		SELECT id::text, chain_id, address, label, signer_key_ref, status, max_inflight_nonces, created_at, updated_at
		FROM wallets
		WHERE chain_id = $1 AND address = $2
	`, chainID, address.Bytes()).Scan(
		&w.ID, &w.ChainID, &addrBytes, &w.Label, &w.SignerKeyRef, &statusStr, &w.MaxInflightNonces, &w.CreatedAt, &w.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Wallet{}, application.ErrNotFound
	}
	if err != nil {
		return domain.Wallet{}, fmt.Errorf("get wallet: %w", err)
	}
	if w.Address, err = bytesToAddress(addrBytes); err != nil {
		return domain.Wallet{}, err
	}
	w.Status = domain.WalletStatus(statusStr)
	return w, nil
}

func (s *Store) ReserveNextNonce(ctx context.Context, chainID int64, wallet domain.Address, intentID string, now time.Time) (domain.NonceReservation, error) {
	var res domain.NonceReservation

	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var statusStr string
		var maxInflight int
		err := tx.QueryRow(ctx, `
			SELECT status, max_inflight_nonces
			FROM wallets
			WHERE chain_id = $1 AND address = $2
		`, chainID, wallet.Bytes()).Scan(&statusStr, &maxInflight)
		if errors.Is(err, pgx.ErrNoRows) {
			return application.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("read wallet for nonce: %w", err)
		}
		if domain.WalletStatus(statusStr) != domain.WalletActive {
			return application.ErrWalletNotActive
		}

		var nextNonce int64
		err = tx.QueryRow(ctx, `
			SELECT next_nonce
			FROM wallet_nonces
			WHERE chain_id = $1 AND wallet_address = $2
			FOR UPDATE
		`, chainID, wallet.Bytes()).Scan(&nextNonce)
		if errors.Is(err, pgx.ErrNoRows) {
			return application.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock wallet_nonces: %w", err)
		}

		var activeCount int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM nonce_reservations
			WHERE chain_id = $1 AND wallet_address = $2 AND status = 'RESERVED'
		`, chainID, wallet.Bytes()).Scan(&activeCount); err != nil {
			return fmt.Errorf("count active nonce reservations: %w", err)
		}
		if activeCount >= maxInflight {
			return application.ErrWalletInflightLimit
		}

		var resID int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO nonce_reservations (chain_id, wallet_address, nonce, intent_id, status, reserved_at, updated_at)
			VALUES ($1, $2, $3, $4, 'RESERVED', $5, $5)
			RETURNING id
		`, chainID, wallet.Bytes(), nextNonce, intentID, now).Scan(&resID); err != nil {
			return fmt.Errorf("insert nonce reservation: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE wallet_nonces
			SET next_nonce = $3, updated_at = $4
			WHERE chain_id = $1 AND wallet_address = $2
		`, chainID, wallet.Bytes(), nextNonce+1, now); err != nil {
			return fmt.Errorf("advance wallet_nonces: %w", err)
		}

		res = domain.NonceReservation{
			ID:            resID,
			ChainID:       chainID,
			WalletAddress: wallet,
			Nonce:         uint64(nextNonce),
			IntentID:      intentID,
			Status:        domain.NonceReserved,
			ReservedAt:    now,
			UpdatedAt:     now,
		}
		return nil
	})
	return res, err
}

func (s *Store) ConfirmNonce(ctx context.Context, chainID int64, wallet domain.Address, nonce uint64, now time.Time) error {
	return s.updateNonceReservationStatus(ctx, chainID, wallet, nonce, domain.NonceConfirmed, now)
}

func (s *Store) ReleaseUnbroadcastNonce(ctx context.Context, chainID int64, wallet domain.Address, nonce uint64, now time.Time) error {
	return s.updateNonceReservationStatus(ctx, chainID, wallet, nonce, domain.NonceReleased, now)
}

func (s *Store) updateNonceReservationStatus(ctx context.Context, chainID int64, wallet domain.Address, nonce uint64, target domain.NonceReservationStatus, now time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE nonce_reservations
		SET status = $4, updated_at = $5
		WHERE chain_id = $1 AND wallet_address = $2 AND nonce = $3 AND status = 'RESERVED'
	`, chainID, wallet.Bytes(), int64(nonce), string(target), now)
	if err != nil {
		return fmt.Errorf("update nonce reservation status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrNotFound
	}
	return nil
}

func (s *Store) ReconcilePendingNonce(ctx context.Context, chainID int64, wallet domain.Address, chainPendingNonce uint64, now time.Time) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		var currentNext int64
		err := tx.QueryRow(ctx, `
			SELECT next_nonce
			FROM wallet_nonces
			WHERE chain_id = $1 AND wallet_address = $2
			FOR UPDATE
		`, chainID, wallet.Bytes()).Scan(&currentNext)
		if errors.Is(err, pgx.ErrNoRows) {
			return application.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock wallet_nonces for reconciliation: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE wallet_nonces
			SET next_nonce = GREATEST(next_nonce, $3),
				last_synced_nonce = $3,
				last_synced_at = $4,
				updated_at = $4
			WHERE chain_id = $1 AND wallet_address = $2
		`, chainID, wallet.Bytes(), int64(chainPendingNonce), now); err != nil {
			return fmt.Errorf("reconcile wallet_nonces: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE nonce_reservations
			SET status = 'CONSUMED_EXTERNAL', updated_at = $4
			WHERE chain_id = $1 AND wallet_address = $2 AND status = 'RESERVED' AND nonce < $3
		`, chainID, wallet.Bytes(), int64(chainPendingNonce), now); err != nil {
			return fmt.Errorf("mark consumed_external reservations: %w", err)
		}
		return nil
	})
}
