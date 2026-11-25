package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/config"
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

	providers := make([]blockchain.ManagedProvider, 0, len(cfg.Chain.RPCURLs))
	clients := make([]blockchain.Client, 0, len(cfg.Chain.RPCURLs))
	for i, rpcURL := range cfg.Chain.RPCURLs {
		name := fmt.Sprintf("rpc-%d", i+1)
		client, err := blockchain.DialGethClient(ctx, name, rpcURL, cfg.Chain.ChainID)
		if err != nil {
			logger.Error("dial rpc provider failed", "provider", name, "error", err)
			os.Exit(1)
		}
		clients = append(clients, client)
		providers = append(providers, blockchain.ManagedProvider{
			Config: blockchain.ProviderConfig{
				Name:           name,
				URL:            rpcURL,
				Priority:       i + 1,
				RequestTimeout: 10 * time.Second,
			},
			Client: client,
		})
	}

	router, err := blockchain.NewRouter(providers)
	if err != nil {
		logger.Error("init rpc router failed", "error", err)
		os.Exit(1)
	}
	defer router.Close()

	verifier, err := blockchain.NewReorgVerifier(clients...)
	if err != nil {
		logger.Error("init reorg verifier failed", "error", err)
		os.Exit(1)
	}

	store := postgres.NewStore(pool)
	monitor := application.NewConfirmationMonitor(store, store, store, router, verifier, application.ConfirmationMonitorConfig{
		ChainID:               cfg.Chain.ChainID,
		RequiredConfirmations: cfg.Chain.RequiredConfirmations,
		PollInterval:          cfg.Monitor.PollInterval,
		BatchSize:             cfg.Monitor.BatchSize,
		StalePendingTTL:       cfg.Monitor.StalePendingTTL,
		Logger:                logger,
	})

	logger.Info("chainflow confirmation-worker listening",
		"chain_id", cfg.Chain.ChainID,
		"required_confirmations", cfg.Chain.RequiredConfirmations,
		"poll_interval", cfg.Monitor.PollInterval,
	)
	if err := monitor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("confirmation-worker exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("chainflow confirmation-worker stopped cleanly")
}
