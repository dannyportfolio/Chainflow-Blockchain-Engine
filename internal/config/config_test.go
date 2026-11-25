package config_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/config"
)

func TestLoadFromMap_DefaultsAndValidation(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"CHAINFLOW_DB_DSN":   "postgres://user:pass@localhost:5432/chainflow?sslmode=disable",
		"CHAINFLOW_RPC_URLS": "https://sepolia.example.invalid,http://localhost:8545",
	}

	cfg, err := config.LoadFromLookup(func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	})
	if err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}

	if cfg.Chain.ChainID != 11155111 {
		t.Fatalf("expected default Sepolia chain ID 11155111, got %d", cfg.Chain.ChainID)
	}
	if cfg.Worker.Concurrency != 20 {
		t.Fatalf("expected default worker concurrency 20, got %d", cfg.Worker.Concurrency)
	}
	if len(cfg.Chain.RPCURLs) != 2 {
		t.Fatalf("expected 2 RPC URLs, got %d", len(cfg.Chain.RPCURLs))
	}
	if cfg.NATS.AckWait <= cfg.Worker.LeaseTTL {
		t.Fatalf("expected AckWait (%s) > LeaseTTL (%s)", cfg.NATS.AckWait, cfg.Worker.LeaseTTL)
	}
	if cfg.Monitor.PollInterval != 2*time.Second {
		t.Fatalf("expected default Monitor.PollInterval 2s, got %s", cfg.Monitor.PollInterval)
	}
	if cfg.Monitor.BatchSize != 100 {
		t.Fatalf("expected default Monitor.BatchSize 100, got %d", cfg.Monitor.BatchSize)
	}
	if cfg.Monitor.StalePendingTTL != 2*time.Minute {
		t.Fatalf("expected default Monitor.StalePendingTTL 2m, got %s", cfg.Monitor.StalePendingTTL)
	}
	if cfg.Monitor.RBFBumpBps != 1500 {
		t.Fatalf("expected default Monitor.RBFBumpBps 1500, got %d", cfg.Monitor.RBFBumpBps)
	}
}

func TestConfigValidate_RejectsInvalidInvariants(t *testing.T) {
	t.Parallel()

	validBase := func() config.Config {
		cfg, err := config.LoadFromLookup(func(key string) (string, bool) {
			switch key {
			case "CHAINFLOW_DB_DSN":
				return "postgres://user:pass@localhost:5432/chainflow?sslmode=disable", true
			case "CHAINFLOW_RPC_URLS":
				return "http://localhost:8545", true
			default:
				return "", false
			}
		})
		if err != nil {
			t.Fatalf("unexpected setup error: %v", err)
		}
		return cfg
	}

	cases := []struct {
		name   string
		mutate func(*config.Config)
	}{
		{
			name: "missing db dsn",
			mutate: func(c *config.Config) {
				c.Database.DSN = ""
			},
		},
		{
			name: "zero worker concurrency",
			mutate: func(c *config.Config) {
				c.Worker.Concurrency = 0
			},
		},
		{
			name: "excessive worker concurrency",
			mutate: func(c *config.Config) {
				c.Worker.Concurrency = 10001
			},
		},
		{
			name: "ack_wait not greater than lease_ttl",
			mutate: func(c *config.Config) {
				c.Worker.LeaseTTL = 30 * time.Second
				c.NATS.AckWait = 30 * time.Second
			},
		},
		{
			name: "min_conns exceeds max_conns",
			mutate: func(c *config.Config) {
				c.Database.MinConns = 30
				c.Database.MaxConns = 10
			},
		},
		{
			name: "missing rpc urls",
			mutate: func(c *config.Config) {
				c.Chain.RPCURLs = nil
			},
		},
		{
			name: "invalid rpc url scheme",
			mutate: func(c *config.Config) {
				c.Chain.RPCURLs = []string{"ftp://bad-rpc"}
			},
		},
		{
			name: "zero chain id",
			mutate: func(c *config.Config) {
				c.Chain.ChainID = 0
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBase()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.name)
			}
		})
	}
}

func TestSecretString_RedactsInLogsAndFormatting(t *testing.T) {
	t.Parallel()

	secret := config.SecretString("postgres://admin:supersecret@db.internal:5432/prod")

	if secret.String() != "[REDACTED]" {
		t.Fatalf("String() leaked secret: %q", secret.String())
	}
	if secret.GoString() != "[REDACTED]" {
		t.Fatalf("GoString() leaked secret: %q", secret.GoString())
	}
	if secret.LogValue().Kind() != slog.KindString || secret.LogValue().String() != "[REDACTED]" {
		t.Fatalf("LogValue() did not redact: %v", secret.LogValue())
	}
	if strings.Contains(secret.String(), "supersecret") {
		t.Fatal("secret material appeared in formatted output")
	}
	if secret.UnsafeValue() != "postgres://admin:supersecret@db.internal:5432/prod" {
		t.Fatal("UnsafeValue() did not return underlying value")
	}
}
