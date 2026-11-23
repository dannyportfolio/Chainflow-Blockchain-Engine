package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/persistence/postgres"
	"github.com/chainflow/chainflow/internal/persistence/storetest"
)

func TestBuildPoolConfig_Validation(t *testing.T) {
	t.Parallel()

	cfg := config.DatabaseConfig{
		DSN:      config.SecretString("postgres://chainflow:chainflow@localhost:5432/chainflow?sslmode=disable"),
		MaxConns: 25,
		MinConns: 5,
	}
	poolCfg, err := postgres.BuildPoolConfig(cfg)
	if err != nil {
		t.Fatalf("BuildPoolConfig failed: %v", err)
	}
	if poolCfg.MaxConns != 25 || poolCfg.MinConns != 5 {
		t.Fatalf("unexpected pool bounds: min=%d max=%d", poolCfg.MinConns, poolCfg.MaxConns)
	}
	if poolCfg.ConnConfig.RuntimeParams["statement_timeout"] != "5000" {
		t.Fatalf("expected 5000ms statement_timeout, got %q", poolCfg.ConnConfig.RuntimeParams["statement_timeout"])
	}
}

func TestPostgresStore_ContractIntegration(t *testing.T) {
	dsn := os.Getenv("CHAINFLOW_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CHAINFLOW_TEST_POSTGRES_DSN not set; skipping live PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, config.DatabaseConfig{
		DSN:      config.SecretString(dsn),
		MaxConns: 10,
		MinConns: 2,
	})
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate failed: %v", err)
	}

	storetest.RunStoreContractTests(t, func(t *testing.T) storetest.StoreBundle {
		t.Helper()
		truncateAll(t, pool)
		s := postgres.NewStore(pool)
		return storetest.StoreBundle{
			Intents:    s,
			Executions: s,
			Nonces:     s,
			Wallets:    s,
			Outbox:     s,
		}
	})
}

func truncateAll(t *testing.T, pool *postgres.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx, `
		TRUNCATE TABLE
			transaction_state_events,
			outbox_events,
			transaction_receipts,
			nonce_reservations,
			transaction_executions,
			transaction_intents,
			wallet_nonces,
			wallets
		RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate tables failed: %v", err)
	}
}
