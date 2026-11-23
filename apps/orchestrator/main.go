package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/chainflow/chainflow/internal/concurrency"
	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/messaging"
	"github.com/chainflow/chainflow/internal/persistence/postgres"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		logger.Error("postgres connect failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool); err != nil {
		logger.Error("database migration failed", "error", err)
		os.Exit(1)
	}

	nc, err := nats.Connect(cfg.NATS.URL)
	if err != nil {
		logger.Error("nats connect failed", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}
	if err := messaging.EnsureStreams(ctx, js, messaging.StreamConfig{
		TXStreamName: cfg.NATS.Stream,
		Storage:      jetstream.FileStorage,
	}); err != nil {
		logger.Error("ensure jetstream streams failed", "error", err)
		os.Exit(1)
	}

	store := postgres.NewStore(pool)
	pub := messaging.NewJetStreamPublisher(js)
	relay := concurrency.NewOutboxRelay(store, pub, concurrency.OutboxRelayConfig{
		PollInterval: 50 * time.Millisecond,
		Logger:       logger,
	})
	sweeper := concurrency.NewOrchestratorSweeper(store, store, store, concurrency.SweeperConfig{
		Interval: 5 * time.Second,
		Logger:   logger,
	})

	errCh := make(chan error, 2)
	go func() { errCh <- relay.Run(ctx) }()
	go func() { errCh <- sweeper.Run(ctx) }()

	logger.Info("chainflow orchestrator running", "nats_url", cfg.NATS.URL, "stream", cfg.NATS.Stream)
	select {
	case <-ctx.Done():
		logger.Info("orchestrator shutdown signal received")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("orchestrator loop failed", "error", err)
			os.Exit(1)
		}
	}
	logger.Info("chainflow orchestrator stopped cleanly")
}
