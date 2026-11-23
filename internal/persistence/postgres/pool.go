package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool = pgxpool.Pool

const migrationAdvisoryLockID int64 = 884201001

func BuildPoolConfig(cfg config.DatabaseConfig) (*pgxpool.Config, error) {
	dsn := cfg.DSN.UnsafeValue()
	if dsn == "" {
		return nil, errors.New("database DSN must not be empty")
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns >= 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.HealthCheckPeriod = 30 * time.Second
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	return poolCfg, nil
}

func NewPool(ctx context.Context, cfg config.DatabaseConfig) (*Pool, error) {
	poolCfg, err := BuildPoolConfig(cfg)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

func Migrate(ctx context.Context, pool *Pool) error {
	migs, err := migrations.LoadUp()
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migrations: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLockID)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    integer PRIMARY KEY,
			name       text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	for _, m := range migs {
		var already bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, m.Version).Scan(&already); err != nil {
			return fmt.Errorf("check migration version %d: %w", m.Version, err)
		}
		if already {
			continue
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.Version, err)
		}
		if _, err := tx.Exec(ctx, m.UpSQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("execute migration %d (%s): %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %d: %w", m.Version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.Version, err)
		}
	}
	return nil
}
