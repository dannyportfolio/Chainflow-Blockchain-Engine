package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/concurrency"
	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/messaging"
	"github.com/chainflow/chainflow/internal/persistence/postgres"
	"github.com/chainflow/chainflow/internal/security/signer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}

	signerKeys, err := parseSignerKeys(os.Getenv("CHAINFLOW_DEV_SIGNER_KEYS"))
	if err != nil || len(signerKeys) == 0 {
		logger.Error("CHAINFLOW_DEV_SIGNER_KEYS must configure at least one key_ref=hex entry", "error", err)
		os.Exit(1)
	}
	devSigner, err := signer.NewDevSigner(signer.DevSignerConfig{
		Keys:         signerKeys,
		MaxFeeCapWei: cfg.Chain.MaxFeeCapWei,
	})
	if err != nil {
		logger.Error("init dev signer failed", "error", err)
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
	for i, rpcURL := range cfg.Chain.RPCURLs {
		name := fmt.Sprintf("rpc-%d", i+1)
		client, err := blockchain.DialGethClient(ctx, name, rpcURL, cfg.Chain.ChainID)
		if err != nil {
			logger.Error("dial rpc provider failed", "provider", name, "error", err)
			os.Exit(1)
		}
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

	cons, err := messaging.EnsureWorkerConsumer(
		ctx,
		js,
		cfg.NATS.Stream,
		"tx-worker-dispatch",
		[]string{messaging.SubjectDispatch, messaging.SubjectRetry},
		cfg.NATS.AckWait,
		cfg.NATS.MaxDeliver,
	)
	if err != nil {
		logger.Error("ensure jetstream consumer failed", "error", err)
		os.Exit(1)
	}

	workerID, _ := os.Hostname()
	if workerID == "" {
		workerID = "tx-worker"
	}

	store := postgres.NewStore(pool)
	gasEst := blockchain.NewGasEstimator(router, blockchain.GasEstimatorConfig{
		CacheTTL:     5 * time.Second,
		MaxFeeCapWei: cfg.Chain.MaxFeeCapWei,
	}, nil)
	processor := concurrency.NewTxProcessor(store, store, store, store, gasEst, router, devSigner, concurrency.TxProcessorConfig{
		WorkerID:   workerID,
		LeaseTTL:   cfg.Worker.LeaseTTL,
		RBFBumpBps: cfg.Monitor.RBFBumpBps,
		Logger:     logger,
	})
	pub := messaging.NewJetStreamPublisher(js)
	poolRunner := concurrency.NewWorkerPool(cons, processor, pub, concurrency.WorkerPoolConfig{
		WorkerID:        workerID,
		Concurrency:     cfg.Worker.Concurrency,
		ShutdownTimeout: cfg.HTTP.ShutdownTimeout,
		Logger:          logger,
	})

	logger.Info("chainflow tx-worker listening", "worker_id", workerID, "concurrency", cfg.Worker.Concurrency, "chain_id", cfg.Chain.ChainID)
	if err := poolRunner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("tx-worker exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("chainflow tx-worker stopped cleanly")
}

func parseSignerKeys(raw string) (map[string]config.SecretString, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	out := make(map[string]config.SecretString)
	for _, item := range strings.Split(trimmed, ",") {
		pair := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(pair) != 2 || strings.TrimSpace(pair[0]) == "" || strings.TrimSpace(pair[1]) == "" {
			return nil, errors.New("expected comma-separated key_ref=hex_key pairs")
		}
		out[strings.TrimSpace(pair[0])] = config.SecretString(strings.TrimSpace(pair[1]))
	}
	return out, nil
}
