package concurrency_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/concurrency"
	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/messaging"
	"github.com/chainflow/chainflow/internal/persistence/memory"
	"github.com/chainflow/chainflow/internal/security/signer"
)

const testPrivKeyHex = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"

func startTestJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("natsserver.NewServer: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("nats server not ready")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := messaging.EnsureStreams(context.Background(), js, messaging.StreamConfig{
		Storage: jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}
	return js
}

func TestEndToEnd_OutboxRelayAndWorkerPool_SimulatedEVM(t *testing.T) {
	t.Parallel()

	key, err := crypto.HexToECDSA(testPrivKeyHex)
	if err != nil {
		t.Fatalf("HexToECDSA: %v", err)
	}
	fromEth := crypto.PubkeyToAddress(key.PublicKey)
	backend := simulated.NewBackend(types.GenesisAlloc{
		fromEth: {Balance: new(big.Int).Mul(big.NewInt(100), big.NewInt(1e18))},
	})
	defer backend.Close()

	simClient := backend.Client()
	chainIDBig, err := simClient.ChainID(context.Background())
	if err != nil {
		t.Fatalf("simClient.ChainID: %v", err)
	}
	chainID := chainIDBig.Int64()

	var fromAddr, toAddr domain.Address
	copy(fromAddr[:], fromEth.Bytes())
	toAddr, _ = domain.ParseAddress("0x000000000000000000000000000000000000dead")

	store := memory.NewStore()
	now := time.Now().UTC()
	if err := store.UpsertWallet(context.Background(), domain.Wallet{
		ID:                "01934a1a-0000-7000-8000-000000000001",
		ChainID:           chainID,
		Address:           fromAddr,
		Label:             "sim-wallet",
		SignerKeyRef:      "dev-1",
		Status:            domain.WalletActive,
		MaxInflightNonces: 16,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, 0); err != nil {
		t.Fatalf("UpsertWallet: %v", err)
	}

	var idCounter atomic.Int64
	svc := application.NewIntentService(application.IntentServiceConfig{
		Intents:             store,
		Executions:          store,
		Wallets:             store,
		ChainID:             chainID,
		DefaultMaxFeeCapWei: big.NewInt(100_000_000_000),
		NewID: func() (string, error) {
			n := idCounter.Add(1)
			return fmt.Sprintf("01934a1a-0000-7000-8000-%012d", n), nil
		},
	})

	const totalIntents = 10
	intentIDs := make([]string, 0, totalIntents)
	for i := 0; i < totalIntents; i++ {
		res, err := svc.Submit(context.Background(), application.SubmitIntentInput{
			TenantID:       "tenant-e2e",
			IdempotencyKey: fmt.Sprintf("e2e-%d", i),
			ChainID:        chainID,
			From:           fromAddr,
			To:             &toAddr,
			ValueWei:       big.NewInt(1_000_000_000_000_000),
		})
		if err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
		intentIDs = append(intentIDs, res.Intent.ID)
	}

	js := startTestJetStream(t)
	pub := messaging.NewJetStreamPublisher(js)
	cons, err := messaging.EnsureWorkerConsumer(
		context.Background(),
		js,
		messaging.DefaultTXStream,
		"tx-worker-e2e",
		[]string{messaging.SubjectDispatch, messaging.SubjectRetry},
		15*time.Second,
		5,
	)
	if err != nil {
		t.Fatalf("EnsureWorkerConsumer: %v", err)
	}

	gethClient := blockchain.NewGethClient("sim", simClient, chainID)
	router, err := blockchain.NewRouter([]blockchain.ManagedProvider{
		{Config: blockchain.ProviderConfig{Name: "sim", Priority: 1}, Client: gethClient},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	gasEst := blockchain.NewGasEstimator(router, blockchain.GasEstimatorConfig{
		CacheTTL:     5 * time.Second,
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	}, nil)
	devSigner, err := signer.NewDevSigner(signer.DevSignerConfig{
		Keys:         map[string]config.SecretString{"dev-1": config.SecretString(testPrivKeyHex)},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	})
	if err != nil {
		t.Fatalf("NewDevSigner: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	processor := concurrency.NewTxProcessor(store, store, store, store, gasEst, router, devSigner, concurrency.TxProcessorConfig{
		WorkerID: "e2e-worker",
		LeaseTTL: 10 * time.Second,
		Logger:   logger,
	})
	relay := concurrency.NewOutboxRelay(store, pub, concurrency.OutboxRelayConfig{
		PollInterval: 20 * time.Millisecond,
		Logger:       logger,
	})
	pool := concurrency.NewWorkerPool(cons, processor, pub, concurrency.WorkerPoolConfig{
		WorkerID:        "e2e-worker",
		Concurrency:     4,
		FetchTimeout:    200 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
		Logger:          logger,
	})

	runCtx, cancelRun := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelRun()

	go func() { _ = relay.Run(runCtx) }()
	go func() { _ = pool.Run(runCtx) }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allBroadcasted := true
		for _, id := range intentIDs {
			got, err := store.GetIntentByIDForSystem(context.Background(), id)
			if err != nil || got.Status != domain.IntentBroadcasted {
				allBroadcasted = false
				break
			}
		}
		if allBroadcasted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancelRun()

	for _, id := range intentIDs {
		got, err := store.GetIntentByIDForSystem(context.Background(), id)
		if err != nil {
			t.Fatalf("GetIntentByIDForSystem(%s): %v", id, err)
		}
		if got.Status != domain.IntentBroadcasted {
			t.Fatalf("intent %s status = %s, want BROADCASTED (err=%s)", id, got.Status, got.LastErrorMessage)
		}
	}

	// Duplicate delivery to TxProcessor must be an idempotent Ack without spawning another execution.
	dupPayload, _ := json.Marshal(messaging.DispatchEnvelope{IntentID: intentIDs[0]})
	dupRes := processor.ProcessMessage(context.Background(), messaging.SubjectDispatch, dupPayload)
	if dupRes.Action != concurrency.ActionAck {
		t.Fatalf("expected ActionAck on duplicate delivery, got %v", dupRes.Action)
	}
	execs, err := store.ListExecutionsByIntent(context.Background(), intentIDs[0])
	if err != nil || len(execs) != 1 {
		t.Fatalf("expected 1 execution after duplicate delivery, got %d (err=%v)", len(execs), err)
	}
}

func TestOrchestratorSweeper_ReclaimsStaleLeaseAndExpiresIntent(t *testing.T) {
	t.Parallel()

	store := memory.NewStore()
	now := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	from, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
	to, _ := domain.ParseAddress("0x2222222222222222222222222222222222222222")
	_ = store.UpsertWallet(context.Background(), domain.Wallet{
		ID:                "01934a1a-0000-7000-8000-000000000010",
		ChainID:           11155111,
		Address:           from,
		Label:             "w1",
		SignerKeyRef:      "k1",
		Status:            domain.WalletActive,
		MaxInflightNonces: 2,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, 0)

	svc := application.NewIntentService(application.IntentServiceConfig{
		Intents:             store,
		Executions:          store,
		Wallets:             store,
		ChainID:             11155111,
		DefaultMaxFeeCapWei: big.NewInt(100_000_000_000),
		Now:                 func() time.Time { return now },
	})

	// 1. Stale lease intent
	staleRes, err := svc.Submit(context.Background(), application.SubmitIntentInput{
		TenantID:       "t1",
		IdempotencyKey: "stale-1",
		ChainID:        11155111,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(1),
		ExpiresAt:      ptrTime(now.Add(10 * time.Minute)),
	})
	if err != nil {
		t.Fatalf("Submit stale: %v", err)
	}
	staleIntent := staleRes.Intent
	expVer := staleIntent.Version
	auditLease, err := domain.TransitionIntent(&staleIntent, domain.IntentTransitionRequest{
		To:         domain.IntentProcessing,
		Actor:      "worker:dead",
		Reason:     "lease_acquired",
		LeaseOwner: "dead-worker",
		LeaseTTL:   5 * time.Second,
		Now:        now,
	})
	if err != nil {
		t.Fatalf("TransitionIntent PROCESSING: %v", err)
	}
	if err := store.TransitionIntentWithOutbox(context.Background(), expVer, staleIntent, auditLease, nil); err != nil {
		t.Fatalf("TransitionIntentWithOutbox: %v", err)
	}

	// 2. Expirable intent
	expRes, err := svc.Submit(context.Background(), application.SubmitIntentInput{
		TenantID:       "t1",
		IdempotencyKey: "expire-1",
		ChainID:        11155111,
		From:           from,
		To:             &to,
		ValueWei:       big.NewInt(1),
		ExpiresAt:      ptrTime(now.Add(5 * time.Second)),
	})
	if err != nil {
		t.Fatalf("Submit expirable: %v", err)
	}

	sweepNow := now.Add(30 * time.Second)
	sweeper := concurrency.NewOrchestratorSweeper(store, store, store, concurrency.SweeperConfig{
		Now: func() time.Time { return sweepNow },
	})
	if err := sweeper.Step(context.Background()); err != nil {
		t.Fatalf("sweeper.Step: %v", err)
	}

	gotStale, _ := store.GetIntentByIDForSystem(context.Background(), staleIntent.ID)
	if gotStale.Status != domain.IntentRetrying {
		t.Fatalf("stale intent status = %s, want RETRYING", gotStale.Status)
	}

	gotExp, _ := store.GetIntentByIDForSystem(context.Background(), expRes.Intent.ID)
	if gotExp.Status != domain.IntentExpired {
		t.Fatalf("expirable intent status = %s, want EXPIRED", gotExp.Status)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
