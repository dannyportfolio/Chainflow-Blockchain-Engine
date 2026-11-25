package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/chainflow/chainflow/internal/api"
	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/observability"
	"github.com/chainflow/chainflow/internal/persistence/postgres"
	"github.com/chainflow/chainflow/internal/security"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		observability.NewLogger("info", "api", os.Stdout).Error("load config failed", "error", err)
		os.Exit(1)
	}
	logger := observability.NewLogger(cfg.LogLevel, "api", os.Stdout)
	metrics := observability.NewMetrics("api")
	_ = observability.InitTracing("chainflow-api")

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

	entries, err := security.ParseAPIKeyEntries(cfg.Auth.APIKeys.UnsafeValue())
	if err != nil {
		logger.Error("parse api keys failed", "error", err)
		os.Exit(1)
	}
	if len(entries) == 0 {
		logger.Error("CHAINFLOW_API_KEYS must configure at least one API key")
		os.Exit(1)
	}
	auth, err := security.NewAPIKeyAuthenticator(entries)
	if err != nil {
		logger.Error("init api key authenticator failed", "error", err)
		os.Exit(1)
	}

	store := postgres.NewStore(pool)
	intentSvc := application.NewIntentService(application.IntentServiceConfig{
		Intents:             store,
		Executions:          store,
		Wallets:             store,
		ChainID:             cfg.Chain.ChainID,
		DefaultMaxFeeCapWei: cfg.Chain.MaxFeeCapWei,
	})
	limiter := security.NewTenantRateLimiter(cfg.Auth.RateLimitPerSec, cfg.Auth.RateLimitBurst, nil)

	handler := api.NewHandler(api.HandlerDeps{
		Intents: intentSvc,
		Auth:    auth,
		Limiter: limiter,
		Logger:  logger,
		Metrics: metrics,
		ReadyCheck: func(c context.Context) error {
			return pool.Ping(c)
		},
	})

	srv := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           handler,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		ReadHeaderTimeout: cfg.HTTP.ReadTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("chainflow api listening", "addr", cfg.HTTP.Addr, "chain_id", cfg.Chain.ChainID)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			logger.Error("http server error", "error", err)
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	logger.Info("chainflow api stopped cleanly")
}
