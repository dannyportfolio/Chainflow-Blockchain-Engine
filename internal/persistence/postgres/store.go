package postgres

import (
	"context"
	"fmt"
	"math/big"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

var (
	_ application.IntentRepository    = (*Store)(nil)
	_ application.ExecutionRepository = (*Store)(nil)
	_ application.NonceCoordinator    = (*Store)(nil)
	_ application.WalletRepository    = (*Store)(nil)
	_ application.OutboxRepository    = (*Store)(nil)
)

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func parseBigInt(raw string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return nil, fmt.Errorf("invalid numeric value %q", raw)
	}
	return v, nil
}

func bytesToAddress(b []byte) (domain.Address, error) {
	if len(b) != 20 {
		return domain.Address{}, fmt.Errorf("invalid address length %d", len(b))
	}
	var a domain.Address
	copy(a[:], b)
	return a, nil
}

func bytesToHash(b []byte) (domain.Hash, error) {
	if len(b) != 32 {
		return domain.Hash{}, fmt.Errorf("invalid hash length %d", len(b))
	}
	var h domain.Hash
	copy(h[:], b)
	return h, nil
}
