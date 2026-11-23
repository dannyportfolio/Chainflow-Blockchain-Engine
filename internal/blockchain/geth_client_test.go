package blockchain

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"

	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/security/signer"
)

const simPrivKeyHex = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"

func TestGethClient_SimulatedEndToEnd(t *testing.T) {
	t.Parallel()

	key, err := crypto.HexToECDSA(simPrivKeyHex)
	if err != nil {
		t.Fatalf("HexToECDSA: %v", err)
	}
	fromEth := crypto.PubkeyToAddress(key.PublicKey)
	toEth := common.HexToAddress("0x000000000000000000000000000000000000dead")

	backend := simulated.NewBackend(types.GenesisAlloc{
		fromEth: {Balance: new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))},
	})
	defer backend.Close()

	ctx := context.Background()
	simClient := backend.Client()
	chainID, err := simClient.ChainID(ctx)
	if err != nil {
		t.Fatalf("simClient.ChainID: %v", err)
	}

	client := NewGethClient("sim", simClient, chainID.Int64())
	if err := client.VerifyChainID(ctx); err != nil {
		t.Fatalf("VerifyChainID: %v", err)
	}

	estimator := NewGasEstimator(client, GasEstimatorConfig{
		CacheTTL:          time.Second,
		GasLimitBufferBps: 1500,
		MaxFeeCapWei:      big.NewInt(100_000_000_000),
	}, nil)

	fees, err := estimator.SuggestFees(ctx)
	if err != nil {
		t.Fatalf("SuggestFees: %v", err)
	}

	var fromDom, toDom domain.Address
	copy(fromDom[:], fromEth.Bytes())
	copy(toDom[:], toEth.Bytes())

	nonce, err := client.PendingNonceAt(ctx, fromDom)
	if err != nil {
		t.Fatalf("PendingNonceAt: %v", err)
	}

	gasLimit, err := estimator.EstimateGasWithBuffer(ctx, CallMsg{
		From:     fromDom,
		To:       &toDom,
		ValueWei: big.NewInt(1_000_000_000_000_000),
	})
	if err != nil {
		t.Fatalf("EstimateGasWithBuffer: %v", err)
	}
	if gasLimit < 21000 {
		t.Fatalf("gasLimit = %d, want >= 21000", gasLimit)
	}

	devSigner, err := signer.NewDevSigner(signer.DevSignerConfig{
		Keys:         map[string]config.SecretString{"sim-key": config.SecretString(simPrivKeyHex)},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	})
	if err != nil {
		t.Fatalf("NewDevSigner: %v", err)
	}

	signedTx, err := devSigner.SignEIP1559(ctx, signer.SignRequest{
		KeyRef:               "sim-key",
		ChainID:              chainID,
		From:                 fromEth,
		To:                   &toEth,
		Nonce:                nonce,
		ValueWei:             big.NewInt(1_000_000_000_000_000),
		GasLimit:             gasLimit,
		MaxPriorityFeePerGas: fees.MaxPriorityFeePerGas,
		MaxFeePerGas:         fees.MaxFeePerGas,
	})
	if err != nil {
		t.Fatalf("SignEIP1559: %v", err)
	}

	precomputedHash, rawTx, err := signer.EncodeSignedTx(signedTx)
	if err != nil {
		t.Fatalf("EncodeSignedTx: %v", err)
	}

	if err := client.SendRawTransaction(ctx, rawTx); err != nil {
		t.Fatalf("SendRawTransaction: %v", err)
	}
	backend.Commit()

	receipt, err := client.TransactionReceipt(ctx, precomputedHash)
	if err != nil {
		t.Fatalf("TransactionReceipt: %v", err)
	}
	if receipt.TxHash != precomputedHash {
		t.Fatalf("receipt.TxHash = %s, want %s", receipt.TxHash.String(), precomputedHash.String())
	}
	if receipt.Status != 1 {
		t.Fatalf("receipt.Status = %d, want 1", receipt.Status)
	}
}

func TestGethClient_VerifyChainIDMismatch(t *testing.T) {
	t.Parallel()

	backend := simulated.NewBackend(types.GenesisAlloc{})
	defer backend.Close()

	client := NewGethClient("sim", backend.Client(), 999999)
	err := client.VerifyChainID(context.Background())
	if !errors.Is(err, ErrChainIDMismatch) {
		t.Fatalf("expected ErrChainIDMismatch, got %v", err)
	}
}

func TestGethClient_ReceiptTxHashMismatch(t *testing.T) {
	t.Parallel()

	requested, _ := domain.ParseHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	spoofed, _ := domain.ParseHash("0x2222222222222222222222222222222222222222222222222222222222222222")

	err := validateReceiptHash(requested, spoofed)
	if err == nil {
		t.Fatal("expected error on mismatched receipt tx_hash")
	}
}

type stubFeeClient struct {
	Client
	calls atomic.Int32
	fee   FeeSuggestion
}

func (s *stubFeeClient) SuggestGasFees(ctx context.Context) (FeeSuggestion, error) {
	s.calls.Add(1)
	return s.fee, nil
}

func TestGasEstimator_SingleflightAndFeeCap(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0)
	stub := &stubFeeClient{
		fee: FeeSuggestion{
			BaseFeeWei:           big.NewInt(10_000_000_000),
			MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
			MaxFeePerGas:         big.NewInt(21_500_000_000),
		},
	}

	est := NewGasEstimator(stub, GasEstimatorConfig{
		CacheTTL:     5 * time.Second,
		MaxFeeCapWei: big.NewInt(50_000_000_000),
	}, func() time.Time { return now })

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := est.SuggestFees(context.Background()); err != nil {
				t.Errorf("SuggestFees: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := stub.calls.Load(); got != 1 {
		t.Fatalf("expected 1 coalesced/cached RPC call, got %d", got)
	}

	cappedEst := NewGasEstimator(stub, GasEstimatorConfig{
		CacheTTL:     0,
		MaxFeeCapWei: big.NewInt(10_000_000_000),
	}, func() time.Time { return now })
	if _, err := cappedEst.SuggestFees(context.Background()); !errors.Is(err, domain.ErrFeeCapExceeded) {
		t.Fatalf("expected domain.ErrFeeCapExceeded, got %v", err)
	}
}

type slowFeeClient struct {
	Client
	started chan struct{}
	release chan struct{}
	fee     FeeSuggestion
}

func (s *slowFeeClient) SuggestGasFees(ctx context.Context) (FeeSuggestion, error) {
	close(s.started)
	select {
	case <-s.release:
		return s.fee, nil
	case <-ctx.Done():
		return FeeSuggestion{}, ctx.Err()
	}
}

func TestGasEstimator_SingleflightIgnoresFirstCallerCancel(t *testing.T) {
	t.Parallel()

	stub := &slowFeeClient{
		started: make(chan struct{}),
		release: make(chan struct{}),
		fee: FeeSuggestion{
			BaseFeeWei:           big.NewInt(10_000_000_000),
			MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
			MaxFeePerGas:         big.NewInt(21_500_000_000),
		},
	}

	est := NewGasEstimator(stub, GasEstimatorConfig{
		CacheTTL:     time.Second,
		MaxFeeCapWei: big.NewInt(50_000_000_000),
	}, nil)

	ctxA, cancelA := context.WithCancel(context.Background())
	errChA := make(chan error, 1)
	go func() {
		_, err := est.SuggestFees(ctxA)
		errChA <- err
	}()

	<-stub.started

	errChB := make(chan error, 1)
	go func() {
		_, err := est.SuggestFees(context.Background())
		errChB <- err
	}()

	cancelA()
	close(stub.release)

	if err := <-errChB; err != nil {
		t.Fatalf("caller B failed after caller A canceled: %v", err)
	}
	if err := <-errChA; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected caller A to observe context.Canceled, got %v", err)
	}
}

type nilBackend struct {
	EthBackend
}

func (nilBackend) ChainID(context.Context) (*big.Int, error) { return nil, nil }
func (nilBackend) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return nil, nil
}
func (nilBackend) TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	return nil, nil
}
func (nilBackend) TransactionByHash(context.Context, common.Hash) (*types.Transaction, bool, error) {
	return nil, false, nil
}

func TestGethClient_NilRPCResponsesDoNotPanic(t *testing.T) {
	t.Parallel()

	c := NewGethClient("nil-node", nilBackend{}, 11155111)
	ctx := context.Background()
	var h domain.Hash

	if _, err := c.ChainID(ctx); err == nil {
		t.Fatal("expected error for nil ChainID")
	}
	if _, err := c.HeaderByNumber(ctx, nil); err == nil {
		t.Fatal("expected error for nil HeaderByNumber")
	}
	if _, err := c.TransactionReceipt(ctx, h); err == nil {
		t.Fatal("expected error for nil TransactionReceipt")
	}
	if _, err := c.TransactionByHash(ctx, h); !errors.Is(err, ErrTransactionNotFound) {
		t.Fatalf("expected ErrTransactionNotFound for nil TransactionByHash, got %v", err)
	}
}
