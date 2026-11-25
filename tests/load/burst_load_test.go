package load_test

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/persistence/memory"
	"github.com/chainflow/chainflow/internal/security/signer"
)

type fastMockRPC struct {
	blockchain.Client
	chainID int64
	sent    atomic.Int64
}

func (m *fastMockRPC) ChainID(_ context.Context) (int64, error) { return m.chainID, nil }
func (m *fastMockRPC) BlockNumber(_ context.Context) (uint64, error) {
	return 1000, nil
}
func (m *fastMockRPC) SuggestGasFees(_ context.Context) (blockchain.FeeSuggestion, error) {
	return blockchain.FeeSuggestion{
		BaseFeeWei:           big.NewInt(1_000_000_000),
		MaxPriorityFeePerGas: big.NewInt(1_000_000_000),
		MaxFeePerGas:         big.NewInt(5_000_000_000),
	}, nil
}
func (m *fastMockRPC) EstimateGas(_ context.Context, _ blockchain.CallMsg) (uint64, error) {
	return 21000, nil
}
func (m *fastMockRPC) SendRawTransaction(_ context.Context, _ []byte) error {
	m.sent.Add(1)
	return nil
}

func TestLoad_10kIntents_100Workers(t *testing.T) {
	const (
		totalIntents = 10000
		numWorkers   = 100
		numWallets   = 20
	)

	ctx := context.Background()
	now := time.Now().UTC()
	store := memory.NewStore()

	keyMap := make(map[string]config.SecretString, numWallets)
	walletAddrs := make([]domain.Address, numWallets)
	for i := 0; i < numWallets; i++ {
		priv, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		hexKey := fmt.Sprintf("0x%x", crypto.FromECDSA(priv))
		keyRef := fmt.Sprintf("dev:w-%d", i)
		keyMap[keyRef] = config.SecretString(hexKey)

		addr, _ := domain.ParseAddress(crypto.PubkeyToAddress(priv.PublicKey).Hex())
		walletAddrs[i] = addr
		_ = store.UpsertWallet(ctx, domain.Wallet{
			ID:                fmt.Sprintf("01934a1a-0000-7000-8000-%012d", i+1),
			ChainID:           11155111,
			Address:           addr,
			Label:             keyRef,
			SignerKeyRef:      keyRef,
			Status:            domain.WalletActive,
			MaxInflightNonces: totalIntents,
			CreatedAt:         now,
			UpdatedAt:         now,
		}, 0)
	}

	devSigner, err := signer.NewDevSigner(signer.DevSignerConfig{
		Keys:         keyMap,
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	})
	if err != nil {
		t.Fatalf("init dev signer: %v", err)
	}

	rpcMock := &fastMockRPC{chainID: 11155111}
	router, err := blockchain.NewRouter([]blockchain.ManagedProvider{{
		Config: blockchain.ProviderConfig{Name: "mock-fast", Priority: 1, MaxInflight: 200, RateLimitRPS: 50000, RateLimitBurst: 50000, RequestTimeout: 2 * time.Second},
		Client: rpcMock,
	}})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	gasEst := blockchain.NewGasEstimator(router, blockchain.GasEstimatorConfig{
		CacheTTL:     5 * time.Second,
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	}, nil)

	toAddr, _ := domain.ParseAddress("0x2222222222222222222222222222222222222222")

	intentSvc := application.NewIntentService(application.IntentServiceConfig{
		Intents:             store,
		Executions:          store,
		Wallets:             store,
		ChainID:             11155111,
		DefaultMaxFeeCapWei: big.NewInt(100_000_000_000),
	})

	jobs := make(chan string, totalIntents)
	for i := 0; i < totalIntents; i++ {
		res, err := intentSvc.Submit(ctx, application.SubmitIntentInput{
			TenantID:       "load-tenant",
			IdempotencyKey: fmt.Sprintf("load-idem-%d", i),
			ChainID:        11155111,
			From:           walletAddrs[i%numWallets],
			To:             &toAddr,
			ValueWei:       big.NewInt(1),
			MaxAttempts:    3,
		})
		if err != nil {
			t.Fatalf("submit intent %d: %v", i, err)
		}
		jobs <- res.Intent.ID
	}
	close(jobs)

	baseGoroutines := runtime.NumGoroutine()
	latencies := make([]time.Duration, 0, totalIntents)
	var latMu sync.Mutex
	var errCount atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerIdx int) {
			defer wg.Done()
			for intentID := range jobs {
				t0 := time.Now()
				intent, err := store.GetIntentByIDForSystem(ctx, intentID)
				if err != nil {
					errCount.Add(1)
					continue
				}
				fees, err := gasEst.SuggestFees(ctx)
				if err != nil {
					errCount.Add(1)
					continue
				}
				nonceRes, err := store.ReserveNextNonce(ctx, intent.ChainID, intent.From, intent.ID, time.Now().UTC())
				if err != nil {
					errCount.Add(1)
					continue
				}
				wallet, err := store.GetWallet(ctx, intent.ChainID, intent.From)
				if err != nil {
					errCount.Add(1)
					continue
				}
				toEth := common.BytesToAddress(toAddr.Bytes())
				signed, err := devSigner.SignEIP1559(ctx, signer.SignRequest{
					KeyRef:               wallet.SignerKeyRef,
					ChainID:              big.NewInt(intent.ChainID),
					From:                 common.BytesToAddress(intent.From.Bytes()),
					To:                   &toEth,
					MaxPriorityFeePerGas: fees.MaxPriorityFeePerGas,
					MaxFeePerGas:         fees.MaxFeePerGas,
					GasLimit:             21000,
					Nonce:                nonceRes.Nonce,
					ValueWei:             intent.ValueWei,
				})
				if err != nil {
					errCount.Add(1)
					continue
				}
				_, raw, err := signer.EncodeSignedTx(signed)
				if err != nil {
					errCount.Add(1)
					continue
				}
				if err := router.SendRawTransaction(ctx, raw); err != nil {
					errCount.Add(1)
					continue
				}
				latMu.Lock()
				latencies = append(latencies, time.Since(t0))
				latMu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if errCount.Load() != 0 {
		t.Fatalf("expected 0 errors, got %d", errCount.Load())
	}
	if rpcMock.sent.Load() != totalIntents {
		t.Fatalf("expected %d sent txs, got %d", totalIntents, rpcMock.sent.Load())
	}

	slices.Sort(latencies)
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	throughput := float64(totalIntents) / elapsed.Seconds()

	t.Logf("LOAD_BENCHMARK intents=%d workers=%d elapsed=%s throughput=%.1f/s p50=%s p95=%s p99=%s goroutines_before=%d goroutines_after=%d",
		totalIntents, numWorkers, elapsed, throughput, p50, p95, p99, baseGoroutines, runtime.NumGoroutine())
}
