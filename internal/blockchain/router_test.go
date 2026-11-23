package blockchain

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/security/signer"
)

type rpcRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcErrorBody   `json:"error,omitempty"`
}

type rpcErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mockRPCServer struct {
	handler http.Handler
	calls   atomic.Int32

	mu          sync.Mutex
	httpStatus  int
	delay       time.Duration
	blockNumber string
	blockHash   string
	rpcErr      *rpcErrorBody
	txByHash    map[string]any
	receipts    map[string]any
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newMockRPCServer(chainIDHex string) *mockRPCServer {
	m := &mockRPCServer{
		httpStatus:  http.StatusOK,
		blockNumber: "0x64", // 100
		blockHash:   "0x1111111111111111111111111111111111111111111111111111111111111111",
		txByHash:    make(map[string]any),
		receipts:    make(map[string]any),
	}
	m.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Method == "eth_chainId" {
			_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: chainIDHex})
			return
		}

		m.calls.Add(1)
		m.mu.Lock()
		status := m.httpStatus
		delay := m.delay
		rpcErr := m.rpcErr
		blockNum := m.blockNumber
		blockHash := m.blockHash
		m.mu.Unlock()

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}

		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("upstream error"))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if rpcErr != nil {
			_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: rpcErr})
			return
		}

		var result any
		switch req.Method {
		case "eth_blockNumber":
			result = blockNum
		case "eth_sendRawTransaction":
			result = "0x0000000000000000000000000000000000000000000000000000000000000001"
		case "eth_getTransactionByHash":
			var h string
			_ = json.Unmarshal(req.Params[0], &h)
			m.mu.Lock()
			result = m.txByHash[h]
			m.mu.Unlock()
		case "eth_getTransactionReceipt":
			var h string
			_ = json.Unmarshal(req.Params[0], &h)
			m.mu.Lock()
			result = m.receipts[h]
			m.mu.Unlock()
		case "eth_getBlockByNumber":
			result = map[string]any{
				"number":           blockNum,
				"hash":             blockHash,
				"parentHash":       "0x0000000000000000000000000000000000000000000000000000000000000000",
				"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
				"logsBloom":        "0x" + strings.Repeat("0", 512),
				"transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
				"stateRoot":        "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
				"receiptsRoot":     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
				"miner":            "0x0000000000000000000000000000000000000000",
				"difficulty":       "0x0",
				"extraData":        "0x",
				"gasLimit":         "0x1c9c380",
				"gasUsed":          "0x0",
				"timestamp":        "0x655f0000",
				"baseFeePerGas":    "0x3b9aca00",
				"transactions":     []any{},
			}
		}
		_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result})
	})
	return m
}

func (m *mockRPCServer) dialClient(ctx context.Context, name string, expectedChainID int64) (*GethClient, error) {
	httpClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if err := req.Context().Err(); err != nil {
				return nil, err
			}
			rec := httptest.NewRecorder()
			m.handler.ServeHTTP(rec, req)
			if err := req.Context().Err(); err != nil {
				return nil, err
			}
			return rec.Result(), nil
		}),
	}
	rc, err := rpc.DialOptions(ctx, "http://mock.local", rpc.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	ec := ethclient.NewClient(rc)
	gc := NewGethClient(name, ec, expectedChainID)
	gc.closeFn = ec.Close
	if err := gc.VerifyChainID(ctx); err != nil {
		gc.Close()
		return nil, err
	}
	return gc, nil
}

func (m *mockRPCServer) Close() {}

func TestRouter_FailoverOn503And429(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		primary := newMockRPCServer("0xaa36a7") // 11155111
		defer primary.Close()
		secondary := newMockRPCServer("0xaa36a7")
		defer secondary.Close()

		ctx := context.Background()
		c1, err := primary.dialClient(ctx, "primary", 11155111)
		if err != nil {
			t.Fatalf("Dial primary: %v", err)
		}
		defer c1.Close()
		c2, err := secondary.dialClient(ctx, "secondary", 11155111)
		if err != nil {
			t.Fatalf("Dial secondary: %v", err)
		}
		defer c2.Close()

		primary.mu.Lock()
		primary.httpStatus = status
		primary.mu.Unlock()

		g1 := NewProviderGuard(ProviderConfig{Name: "primary", Priority: 1, FailureThreshold: 1, OpenCooldown: time.Minute}, nil)
		g2 := NewProviderGuard(ProviderConfig{Name: "secondary", Priority: 2, FailureThreshold: 2, OpenCooldown: time.Minute}, nil)

		router, err := NewRouter([]ManagedProvider{
			{Config: ProviderConfig{Name: "primary", Priority: 1}, Client: c1, Guard: g1},
			{Config: ProviderConfig{Name: "secondary", Priority: 2}, Client: c2, Guard: g2},
		})
		if err != nil {
			t.Fatalf("NewRouter: %v", err)
		}

		bn, err := router.BlockNumber(ctx)
		if err != nil {
			t.Fatalf("BlockNumber failover failed: %v", err)
		}
		if bn != 100 {
			t.Fatalf("BlockNumber = %d, want 100", bn)
		}
		if g1.State() != CircuitOpen {
			t.Fatalf("primary circuit = %s, want CircuitOpen", g1.State())
		}
	}
}

func TestRouter_NoFailoverOnDeterministicError(t *testing.T) {
	t.Parallel()

	primary := newMockRPCServer("0xaa36a7")
	defer primary.Close()
	secondary := newMockRPCServer("0xaa36a7")
	defer secondary.Close()

	ctx := context.Background()
	c1, _ := primary.dialClient(ctx, "primary", 11155111)
	defer c1.Close()
	c2, _ := secondary.dialClient(ctx, "secondary", 11155111)
	defer c2.Close()

	primary.mu.Lock()
	primary.rpcErr = &rpcErrorBody{Code: -32000, Message: "nonce too low"}
	primary.mu.Unlock()

	router, err := NewRouter([]ManagedProvider{
		{Config: ProviderConfig{Name: "primary", Priority: 1}, Client: c1, Guard: NewProviderGuard(ProviderConfig{Name: "primary", FailureThreshold: 1}, nil)},
		{Config: ProviderConfig{Name: "secondary", Priority: 2}, Client: c2, Guard: NewProviderGuard(ProviderConfig{Name: "secondary", FailureThreshold: 1}, nil)},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	_, err = router.BlockNumber(ctx)
	var ce *ClassifiedError
	if !errors.As(err, &ce) || ce.Kind != ErrKindNonceTooLow {
		t.Fatalf("expected ErrKindNonceTooLow, got %v", err)
	}
	if secondary.calls.Load() != 0 {
		t.Fatalf("secondary should not be called on deterministic error, got %d calls", secondary.calls.Load())
	}
}

func TestRouter_AmbiguousTimeoutReconciliation(t *testing.T) {
	t.Parallel()

	primary := newMockRPCServer("0xaa36a7")
	defer primary.Close()
	secondary := newMockRPCServer("0xaa36a7")
	defer secondary.Close()

	ctx := context.Background()
	c1, _ := primary.dialClient(ctx, "primary", 11155111)
	defer c1.Close()
	c2, _ := secondary.dialClient(ctx, "secondary", 11155111)
	defer c2.Close()

	devSigner, err := signer.NewDevSigner(signer.DevSignerConfig{
		Keys:         map[string]config.SecretString{"k": config.SecretString(simPrivKeyHex)},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	})
	if err != nil {
		t.Fatalf("NewDevSigner: %v", err)
	}
	from, _ := devSigner.Address(ctx, "k")
	to := common.HexToAddress("0x000000000000000000000000000000000000dead")
	tx, err := devSigner.SignEIP1559(ctx, signer.SignRequest{
		KeyRef:               "k",
		ChainID:              big.NewInt(11155111),
		From:                 from,
		To:                   &to,
		Nonce:                1,
		ValueWei:             big.NewInt(1),
		GasLimit:             21000,
		MaxPriorityFeePerGas: big.NewInt(1_000_000_000),
		MaxFeePerGas:         big.NewInt(10_000_000_000),
	})
	if err != nil {
		t.Fatalf("SignEIP1559: %v", err)
	}
	txHash, rawTx, _ := signer.EncodeSignedTx(tx)

	// Primary times out on broadcast, but Secondary already sees the tx in mempool.
	primary.mu.Lock()
	primary.delay = 200 * time.Millisecond
	primary.mu.Unlock()

	v, r, s := tx.RawSignatureValues()
	secondary.mu.Lock()
	secondary.txByHash[txHash.String()] = map[string]any{
		"hash":                 txHash.String(),
		"nonce":                "0x1",
		"from":                 from.Hex(),
		"to":                   to.Hex(),
		"value":                "0x1",
		"gas":                  "0x5208",
		"gasPrice":             "0x2540be400",
		"maxPriorityFeePerGas": "0x3b9aca00",
		"maxFeePerGas":         "0x2540be400",
		"input":                "0x",
		"type":                 "0x2",
		"chainId":              "0xaa36a7",
		"v":                    "0x" + v.Text(16),
		"r":                    "0x" + r.Text(16),
		"s":                    "0x" + s.Text(16),
	}
	secondary.mu.Unlock()

	router, _ := NewRouter([]ManagedProvider{
		{Config: ProviderConfig{Name: "primary", Priority: 1}, Client: c1, Guard: NewProviderGuard(ProviderConfig{Name: "primary", RequestTimeout: 30 * time.Millisecond, FailureThreshold: 1}, nil)},
		{Config: ProviderConfig{Name: "secondary", Priority: 2}, Client: c2, Guard: NewProviderGuard(ProviderConfig{Name: "secondary", RequestTimeout: time.Second, FailureThreshold: 2}, nil)},
	})

	// SendRawTransaction times out on primary after primary accepted it; Router reconciles via secondary without re-broadcasting.
	if err := router.SendRawTransaction(ctx, rawTx); err != nil {
		t.Fatalf("SendRawTransaction with timeout reconciliation: %v", err)
	}
	if calls := secondary.calls.Load(); calls != 1 {
		// Expect only eth_getTransactionByHash on secondary, no eth_sendRawTransaction re-broadcast.
		t.Fatalf("expected 1 RPC call on secondary (reconcile only), got %d", calls)
	}
}

func TestRouter_LaggingPrimaryCrossCheck(t *testing.T) {
	t.Parallel()

	primary := newMockRPCServer("0xaa36a7")
	defer primary.Close()
	secondary := newMockRPCServer("0xaa36a7")
	defer secondary.Close()

	ctx := context.Background()
	c1, _ := primary.dialClient(ctx, "primary", 11155111)
	defer c1.Close()
	c2, _ := secondary.dialClient(ctx, "secondary", 11155111)
	defer c2.Close()

	txHash, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	blockHash, _ := domain.ParseHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	// Primary lags at block 100 (0x64) and has no receipt; Secondary is at block 105 (0x69) with receipt at block 103 (0x67).
	secondary.mu.Lock()
	secondary.blockNumber = "0x69"
	secondary.blockHash = blockHash.String()
	secondary.receipts[txHash.String()] = map[string]any{
		"transactionHash":   txHash.String(),
		"transactionIndex":  "0x0",
		"blockHash":         blockHash.String(),
		"blockNumber":       "0x67",
		"from":              "0x0000000000000000000000000000000000000001",
		"to":                "0x0000000000000000000000000000000000000002",
		"cumulativeGasUsed": "0x5208",
		"gasUsed":           "0x5208",
		"effectiveGasPrice": "0x3b9aca00",
		"logs":              []any{},
		"logsBloom":         "0x" + strings.Repeat("0", 512),
		"status":            "0x1",
		"type":              "0x2",
	}
	secondary.mu.Unlock()

	router, _ := NewRouter([]ManagedProvider{
		{Config: ProviderConfig{Name: "primary", Priority: 1}, Client: c1, Guard: NewProviderGuard(ProviderConfig{Name: "primary"}, nil)},
		{Config: ProviderConfig{Name: "secondary", Priority: 2}, Client: c2, Guard: NewProviderGuard(ProviderConfig{Name: "secondary"}, nil)},
	})

	receipt, prov, err := router.CrossCheckReceipt(ctx, txHash, 103)
	if err != nil {
		t.Fatalf("CrossCheckReceipt: %v", err)
	}
	if prov != "secondary" || receipt.BlockNumber != 103 || receipt.Status != 1 {
		t.Fatalf("got prov=%q receipt=%+v", prov, receipt)
	}

	block105 := uint64(105)
	secHeader, err := c2.HeaderByNumber(ctx, &block105)
	if err != nil {
		t.Fatalf("c2.HeaderByNumber: %v", err)
	}
	ok, err := router.CrossCheckBlockHash(ctx, 105, secHeader.Hash)
	if err != nil {
		t.Fatalf("CrossCheckBlockHash: %v", err)
	}
	if !ok {
		t.Fatal("expected CrossCheckBlockHash to confirm canonical blockHash on secondary")
	}
}
