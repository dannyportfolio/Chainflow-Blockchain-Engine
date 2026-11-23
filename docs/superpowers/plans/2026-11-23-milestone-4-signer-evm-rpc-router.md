# Milestone 4: Isolated Signer, EVM Client & RPC Failover Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the isolated `Signer` boundary with a testnet-guarded ECDSA dev signer, the `go-ethereum` EVM client adapter with EIP-1559 fee estimation, and the multi-provider RPC failover router with per-provider circuit breakers, semaphores, rate limits, and cross-check verification.

**Architecture:** `internal/security/signer` owns private key custody behind a narrow `Signer` interface (`SignEIP1559`, `Address`) that never exports key material and enforces `chain_id != 1`, `from` address, and `max_fee_cap_wei` invariants. `internal/blockchain` defines a narrow `Client` port, normalizes provider-specific JSON-RPC errors into `ClassifiedError`, wraps `go-ethereum`'s `ethclient`/`rpc.Client` in `GethClient` + `singleflight`-coalesced `GasEstimator`, and routes calls across prioritized providers guarded by per-provider `ProviderGuard` (semaphore + token bucket + circuit breaker) with cross-provider verification for lagging heads and ambiguous broadcast timeouts.

**Tech Stack:** Go `1.27+`, `github.com/ethereum/go-ethereum` (`ethclient`, `ethclient/simulated`, `core/types`, `crypto`, `rpc`), `golang.org/x/sync/singleflight`, `net/http/httptest`.

**Spec:** [07-roadmap.md](../../architecture/07-roadmap.md) (Milestone M4), [ADR-0007](../../adr/0007-multi-provider-evm-rpc-failover-and-hedging.md), [ADR-0008](../../adr/0008-isolated-signer-boundary.md), [06-concurrency-nonce-and-failures.md](../../architecture/06-concurrency-nonce-and-failures.md), [threat-model.md](../../security/threat-model.md)

## Global Constraints

- Go `1.27+` toolchain (`go vet ./...` and `go test -race -count=1 ./...` must pass).
- Private keys must never appear in logs, `String()`, `GoString()`, `slog.LogValue()`, metrics, traces, or error messages (`threat-model.md` §3 I1).
- `DevSigner` must refuse to sign for Ethereum Mainnet (`chain_id == 1`) unless `AllowMainnet` is explicitly `true` (`ADR-0008`, `threat-model.md` §4).
- `internal/domain` must remain zero-external-dependency (no `go-ethereum` imports in `internal/domain`).
- All RPC calls and locks must respect `context.Context` cancellation and deadlines.

## Review Focus

1. **Accidental mainnet signing (`chain_id == 1`):** `DevSigner.SignEIP1559` must reject `chain_id == 1` when `AllowMainnet == false` (pinned in Task 2).
2. **Private key leakage in logs/errors:** `DevSigner` `String()`, `slog` output, and malformed-key errors must never contain private key hex (pinned in Task 2).
3. **Spoofed/mismatched chain or receipt from RPC:** `GethClient` must reject `eth_chainId != expectedChainID` and `TransactionReceipt` where `receipt.TxHash != requestedTxHash` (pinned in Task 3).
4. **Deterministic RPC errors tripping circuit breakers:** `nonce too low`, `replacement transaction underpriced`, `already known`, and `insufficient funds` must NOT trip a provider's circuit breaker or trigger failover (pinned in Tasks 4 & 5).
5. **Lagging primary RPC or ambiguous broadcast timeout:** Suspected missing receipt on a lagging primary must cross-check a healthy secondary provider, and `SendRawTransaction` timeout must reconcile via `TransactionByHash` without re-broadcasting blindly (pinned in Task 5).

---

### Task 1: Add `go-ethereum` Dependency, `blockchain.Client` Port & RPC Error Classification

**Files:**
- Modify: `go.mod`, `go.sum`
- Create: `internal/blockchain/types.go`
- Create: `internal/blockchain/errors.go`
- Test: `internal/blockchain/errors_test.go`

**Interfaces:**
- Consumes: `domain.Address`, `domain.Hash`
- Produces:
  - `blockchain.RPCErrorKind` (`ErrKindTransient`, `ErrKindRateLimit`, `ErrKindTimeout`, `ErrKindNonceTooLow`, `ErrKindUnderpriced`, `ErrKindAlreadyKnown`, `ErrKindInsufficientFunds`, `ErrKindPermanent`)
  - `blockchain.ClassifiedError` (`Provider string`, `Kind RPCErrorKind`, `Err error`; `Error() string`, `Unwrap() error`, `Retryable() bool`, `TripsCircuit() bool`)
  - `blockchain.ClassifyRPCError(provider string, err error) *ClassifiedError`
  - `blockchain.FeeSuggestion`, `blockchain.CallMsg`, `blockchain.Header`, `blockchain.Receipt`, `blockchain.ChainTransaction`
  - `blockchain.Client` interface
  - Sentinel errors: `ErrReceiptNotFound`, `ErrTransactionNotFound`, `ErrChainIDMismatch`, `ErrCircuitOpen`, `ErrNoHealthyProviders`

- [ ] **Step 1: Add `github.com/ethereum/go-ethereum` to `go.mod`**

Run: `go get github.com/ethereum/go-ethereum@latest`
Expected: `go.mod` updated with `github.com/ethereum/go-ethereum` and `go.sum` populated without errors.

- [ ] **Step 2: Write the failing test in `internal/blockchain/errors_test.go`**

```go
package blockchain

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestClassifyRPCError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		err          error
		wantKind     RPCErrorKind
		wantRetry    bool
		wantTripsCB  bool
	}{
		{name: "nonce too low", err: errors.New("nonce too low: next nonce 5, tx nonce 4"), wantKind: ErrKindNonceTooLow, wantRetry: false, wantTripsCB: false},
		{name: "replacement underpriced", err: errors.New("replacement transaction underpriced"), wantKind: ErrKindUnderpriced, wantRetry: false, wantTripsCB: false},
		{name: "tx underpriced", err: errors.New("transaction underpriced"), wantKind: ErrKindUnderpriced, wantRetry: false, wantTripsCB: false},
		{name: "already known", err: errors.New("already known"), wantKind: ErrKindAlreadyKnown, wantRetry: false, wantTripsCB: false},
		{name: "known transaction", err: errors.New("known transaction: 0xabc"), wantKind: ErrKindAlreadyKnown, wantRetry: false, wantTripsCB: false},
		{name: "insufficient funds", err: errors.New("insufficient funds for gas * price + value"), wantKind: ErrKindInsufficientFunds, wantRetry: false, wantTripsCB: false},
		{name: "http 429", err: &HTTPError{StatusCode: http.StatusTooManyRequests, Body: "rate limit"}, wantKind: ErrKindRateLimit, wantRetry: true, wantTripsCB: true},
		{name: "http 503", err: &HTTPError{StatusCode: http.StatusServiceUnavailable, Body: "unavailable"}, wantKind: ErrKindTransient, wantRetry: true, wantTripsCB: true},
		{name: "deadline exceeded", err: context.DeadlineExceeded, wantKind: ErrKindTimeout, wantRetry: true, wantTripsCB: true},
		{name: "nil returns nil", err: nil, wantKind: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyRPCError("alchemy", tc.err)
			if tc.err == nil {
				if got != nil {
					t.Fatalf("expected nil, got %+v", got)
				}
				return
			}
			if got.Kind != tc.wantKind {
				t.Fatalf("kind = %q, want %q", got.Kind, tc.wantKind)
			}
			if got.Retryable() != tc.wantRetry {
				t.Fatalf("Retryable() = %v, want %v", got.Retryable(), tc.wantRetry)
			}
			if got.TripsCircuit() != tc.wantTripsCB {
				t.Fatalf("TripsCircuit() = %v, want %v", got.TripsCircuit(), tc.wantTripsCB)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("expected errors.Is to unwrap original error")
			}
		})
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test -v ./internal/blockchain/...`
Expected: FAIL with undefined `ClassifyRPCError`, `RPCErrorKind`, `HTTPError`.

- [ ] **Step 4: Implement `internal/blockchain/types.go` and `internal/blockchain/errors.go`**

In `internal/blockchain/types.go`, define `FeeSuggestion`, `CallMsg`, `Header`, `Receipt`, `ChainTransaction`, and the `Client` interface:
- `ChainID(ctx context.Context) (int64, error)`
- `BlockNumber(ctx context.Context) (uint64, error)`
- `HeaderByNumber(ctx context.Context, number *uint64) (Header, error)`
- `PendingNonceAt(ctx context.Context, addr domain.Address) (uint64, error)`
- `LatestNonceAt(ctx context.Context, addr domain.Address) (uint64, error)`
- `SuggestGasFees(ctx context.Context) (FeeSuggestion, error)`
- `EstimateGas(ctx context.Context, msg CallMsg) (uint64, error)`
- `SendRawTransaction(ctx context.Context, rawTx []byte) error`
- `TransactionReceipt(ctx context.Context, txHash domain.Hash) (Receipt, error)`
- `TransactionByHash(ctx context.Context, txHash domain.Hash) (ChainTransaction, error)`
- `Close()`

In `internal/blockchain/errors.go`, define `RPCErrorKind`, sentinel errors (`ErrReceiptNotFound`, `ErrTransactionNotFound`, `ErrChainIDMismatch`, `ErrCircuitOpen`, `ErrNoHealthyProviders`), `HTTPError` (and unwrap `rpc.HTTPError` from `go-ethereum/rpc` via `errors.As`), `ClassifiedError`, and `ClassifyRPCError(provider string, err error) *ClassifiedError` matching case-insensitive substrings and HTTP status codes (`429` -> `ErrKindRateLimit`, `500..599` -> `ErrKindTransient`, `context.DeadlineExceeded` / `"timeout"` -> `ErrKindTimeout`).

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race -v ./internal/blockchain/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/blockchain/types.go internal/blockchain/errors.go internal/blockchain/errors_test.go
git commit -m "feat(blockchain): add Client port, domain types, and RPC error classification"
```

---

### Task 2: Isolated `Signer` Interface & `DevSigner` (`internal/security/signer`)

**Files:**
- Create: `internal/security/signer/signer.go`
- Test: `internal/security/signer/signer_test.go`

**Interfaces:**
- Consumes: `config.SecretString`, `domain.ErrFeeCapExceeded`, `domain.ErrInvalidInput`
- Produces:
  - `signer.SignRequest` (`KeyRef string`, `ChainID *big.Int`, `From common.Address`, `To *common.Address`, `Nonce uint64`, `ValueWei *big.Int`, `Data []byte`, `GasLimit uint64`, `MaxPriorityFeePerGas *big.Int`, `MaxFeePerGas *big.Int`)
  - `signer.Signer` interface (`SignEIP1559(ctx context.Context, req SignRequest) (*types.Transaction, error)`, `Address(ctx context.Context, keyRef string) (common.Address, error)`)
  - `signer.DevSignerConfig` (`Keys map[string]config.SecretString`, `MaxFeeCapWei *big.Int`, `AllowMainnet bool`)
  - `signer.NewDevSigner(cfg DevSignerConfig) (*DevSigner, error)`
  - `signer.EncodeSignedTx(tx *types.Transaction) (domain.Hash, []byte, error)`
  - Sentinel errors: `ErrKeyNotFound`, `ErrMainnetSigningForbidden`, `ErrFromAddressMismatch`

- [ ] **Step 1: Write the failing tests in `internal/security/signer/signer_test.go`**

Include tests covering:
- `TestDevSigner_SignEIP1559_Success`: signs on Sepolia (`11155111`), recovers sender with `types.Sender(types.NewLondonSigner(chainID), tx) == fromAddr`, verifies `EncodeSignedTx` returns matching `tx.Hash()` and `MarshalBinary()` bytes.
- `TestDevSigner_MainnetGuard`: `chain_id = 1` with `AllowMainnet: false` returns `ErrMainnetSigningForbidden`; with `AllowMainnet: true` succeeds.
- `TestDevSigner_Guardrails`: tests `From` mismatch (`ErrFromAddressMismatch`), unknown `KeyRef` (`ErrKeyNotFound`), `MaxFeePerGas > MaxFeeCapWei` (`domain.ErrFeeCapExceeded`), `MaxPriorityFeePerGas > MaxFeePerGas`, `GasLimit < 21000`, `ChainID <= 0`, and cancelled `ctx`.
- `TestDevSigner_ZeroKeyLeakage`: verifies `fmt.Sprint(s)`, `fmt.Sprintf("%#v", s)`, `slog` JSON output of `slog.Any("signer", s)`, and invalid-key error messages do NOT contain any substring of the raw private key hex.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./internal/security/signer/...`
Expected: FAIL (package does not exist).

- [ ] **Step 3: Implement `internal/security/signer/signer.go`**

Parse hex keys at construction (accepting optional `0x` prefix via `crypto.HexToECDSA`), never include key hex in any error, store `map[string]devKeyEntry` (`key *ecdsa.PrivateKey`, `addr common.Address`), implement `String() string`, `GoString() string`, and `LogValue() slog.Value` returning `"[REDACTED]"`, and enforce all `SignRequest` guardrails before calling `types.SignNewTx(entry.key, types.NewLondonSigner(req.ChainID), &types.DynamicFeeTx{...})`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race -v ./internal/security/signer/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/security/signer/signer.go internal/security/signer/signer_test.go
git commit -m "feat(signer): add isolated Signer port and testnet-guarded DevSigner"
```

---

### Task 3: `go-ethereum` Client Adapter (`GethClient`) & EIP-1559 `GasEstimator`

**Files:**
- Create: `internal/blockchain/geth_client.go`
- Create: `internal/blockchain/gas.go`
- Test: `internal/blockchain/geth_client_test.go`

**Interfaces:**
- Consumes: `blockchain.Client`, `blockchain.ClassifyRPCError`, `signer.Signer`, `signer.EncodeSignedTx`, `domain.ErrFeeCapExceeded`
- Produces:
  - `blockchain.DialGethClient(ctx context.Context, name, rawURL string, expectedChainID int64) (*GethClient, error)`
  - `blockchain.NewGethClient(name string, eth *ethclient.Client, raw *rpc.Client, expectedChainID int64) *GethClient`
  - `blockchain.GasEstimatorConfig` (`CacheTTL time.Duration`, `GasLimitBufferBps uint64`, `MinPriorityFeeWei *big.Int`, `MaxFeeCapWei *big.Int`)
  - `blockchain.NewGasEstimator(client Client, cfg GasEstimatorConfig, now func() time.Time) *GasEstimator`
  - `(g *GasEstimator) SuggestFees(ctx context.Context) (FeeSuggestion, error)`
  - `(g *GasEstimator) EstimateGasWithBuffer(ctx context.Context, msg CallMsg) (uint64, error)`

- [ ] **Step 1: Write the failing tests in `internal/blockchain/geth_client_test.go`**

Using `github.com/ethereum/go-ethereum/ethclient/simulated`:
- `TestGethClient_SimulatedEndToEnd`: fund a `DevSigner` address in `simulated.NewBackend`, create `GethClient` via `NewGethClient`, call `SuggestFees` & `EstimateGasWithBuffer`, sign an EIP-1559 transfer with `DevSigner`, compute `precomputedHash, rawTx, _ := signer.EncodeSignedTx(tx)`, call `SendRawTransaction(ctx, rawTx)`, call `backend.Commit()`, fetch `TransactionReceipt(ctx, precomputedHash)`, and assert `receipt.TxHash == precomputedHash` and `receipt.Status == 1`.
- `TestGethClient_VerifyChainIDMismatch`: pass wrong `expectedChainID` to `VerifyChainID` and assert `errors.Is(err, ErrChainIDMismatch)`.
- `TestGethClient_ReceiptTxHashMismatch`: verify `validateReceipt` rejects a receipt whose `TxHash` does not match the requested hash.
- `TestGasEstimator_SingleflightAndFeeCap`: verify concurrent `SuggestFees` calls within `CacheTTL` coalesce onto a single underlying RPC call, and fees exceeding `MaxFeeCapWei` return `domain.ErrFeeCapExceeded`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./internal/blockchain/...`
Expected: FAIL with undefined `NewGethClient`, `NewGasEstimator`.

- [ ] **Step 3: Implement `internal/blockchain/geth_client.go` and `internal/blockchain/gas.go`**

- `GethClient`: wraps `*ethclient.Client` + `*rpc.Client`; `SendRawTransaction` sends `eth_sendRawTransaction` with `hexutil.Encode(rawTx)`; `SuggestGasFees` queries `eth_feeHistory` (5 blocks, `[25, 50, 75]` percentiles) with fallback to `SuggestGasTipCap` + latest header `BaseFee`; `TransactionReceipt` checks `receipt.TxHash == txHash`; all RPC errors pass through `ClassifyRPCError(g.name, err)`.
- `GasEstimator`: uses `singleflight.Group` + `sync.RWMutex` TTL cache (`CacheTTL`, default `1s`) for `SuggestFees`, enforces `MaxFeeCapWei` (`domain.ErrFeeCapExceeded`), and applies `GasLimitBufferBps` (default `1500` = +15%) in `EstimateGasWithBuffer`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race -v ./internal/blockchain/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/blockchain/geth_client.go internal/blockchain/gas.go internal/blockchain/geth_client_test.go
git commit -m "feat(blockchain): implement GethClient adapter and singleflight EIP-1559 GasEstimator"
```

---

### Task 4: Per-Provider Circuit Breaker, Semaphore & Token Bucket (`ProviderGuard`)

**Files:**
- Create: `internal/blockchain/provider_guard.go`
- Test: `internal/blockchain/provider_guard_test.go`

**Interfaces:**
- Consumes: `blockchain.ClassifiedError`, `blockchain.ErrCircuitOpen`
- Produces:
  - `blockchain.CircuitState` (`CircuitClosed`, `CircuitOpen`, `CircuitHalfOpen`)
  - `blockchain.ProviderConfig` (`Name string`, `URL string`, `Priority int`, `MaxInflight int`, `RateLimitRPS float64`, `RateLimitBurst int`, `FailureThreshold int`, `OpenCooldown time.Duration`, `RequestTimeout time.Duration`)
  - `blockchain.NewProviderGuard(cfg ProviderConfig, now func() time.Time) *ProviderGuard`
  - `(g *ProviderGuard) Do(ctx context.Context, fn func(context.Context) error) error`
  - `(g *ProviderGuard) State() CircuitState`
  - `(g *ProviderGuard) Name() string`

- [ ] **Step 1: Write the failing tests in `internal/blockchain/provider_guard_test.go`**

Include tests with injectable fake clock:
- `TestProviderGuard_CircuitBreakerLifecycle`: `FailureThreshold: 3` -> 3 transient/5xx errors open circuit; 4th call returns `ErrCircuitOpen` without invoking `fn`; advancing fake clock by `OpenCooldown` transitions to `CircuitHalfOpen` allowing 1 probe; probe failure re-opens; next cooldown + probe success transitions to `CircuitClosed`.
- `TestProviderGuard_DeterministicErrorsDoNotTripCircuit`: 10 consecutive `ClassifiedError{Kind: ErrKindNonceTooLow}` and `ErrKindUnderpriced` keep `State() == CircuitClosed`.
- `TestProviderGuard_BoundedInflightAndRateLimit`: `MaxInflight: 2` under 10 concurrent goroutines with `-race` never exceeds 2 concurrent executions inside `fn`, and token bucket rejects/blocks when exhausted.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./internal/blockchain/...`
Expected: FAIL with undefined `ProviderConfig`, `NewProviderGuard`.

- [ ] **Step 3: Implement `internal/blockchain/provider_guard.go`**

Implement `ProviderGuard` with:
- Buffered `chan struct{}` semaphore (`cap = MaxInflight`) acquired with `select` on `ctx.Done()`.
- Mutex-guarded token bucket refill using `now()`.
- Mutex-guarded circuit breaker (`Closed` / `Open` / `HalfOpen` with single in-flight probe lock), tripping only when `err` is retryable / `TripsCircuit() == true`.
- Per-call `context.WithTimeout(ctx, cfg.RequestTimeout)` when `RequestTimeout > 0`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race -v ./internal/blockchain/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/blockchain/provider_guard.go internal/blockchain/provider_guard_test.go
git commit -m "feat(blockchain): add per-provider circuit breaker, semaphore, and rate limiter"
```

---

### Task 5: Multi-Provider RPC Failover Router & Cross-Check Verification

**Files:**
- Create: `internal/blockchain/router.go`
- Test: `internal/blockchain/router_test.go`

**Interfaces:**
- Consumes: `blockchain.Client`, `blockchain.ProviderGuard`, `blockchain.ClassifyRPCError`
- Produces:
  - `blockchain.ManagedProvider` (`Config ProviderConfig`, `Client Client`, `Guard *ProviderGuard`)
  - `blockchain.NewRouter(providers []ManagedProvider) (*Router, error)` (implements `blockchain.Client`)
  - `(r *Router) ReconcileTransaction(ctx context.Context, txHash domain.Hash) (ChainTransaction, bool, error)`
  - `(r *Router) CrossCheckReceipt(ctx context.Context, txHash domain.Hash, minBlock uint64) (Receipt, string, error)`
  - `(r *Router) CrossCheckBlockHash(ctx context.Context, blockNumber uint64, expectedHash domain.Hash) (bool, error)`

- [ ] **Step 1: Write the failing tests in `internal/blockchain/router_test.go`**

Using `net/http/httptest` JSON-RPC mock servers wired to `DialGethClient`:
- `TestRouter_FailoverOn503And429`: Primary returns `503` (and `429` in a second subtest), Secondary returns valid `eth_blockNumber` / `eth_sendRawTransaction` -> `Router` fails over to Secondary, returns success, and Primary's circuit opens after `FailureThreshold`.
- `TestRouter_NoFailoverOnDeterministicError`: Primary returns `"nonce too low"` -> `Router` returns `ClassifiedError{Kind: ErrKindNonceTooLow}` immediately and Secondary call count remains `0`.
- `TestRouter_AmbiguousTimeoutReconciliation`: Primary accepts `eth_sendRawTransaction` then delays past `RequestTimeout` (returning `ErrKindTimeout`); `ReconcileTransaction` queries providers via `eth_getTransactionByHash`, finds the tx, and returns `(tx, true, nil)` without re-broadcasting.
- `TestRouter_LaggingPrimaryCrossCheck`: Primary reports `eth_blockNumber = 100` and `eth_getTransactionReceipt = null`; Secondary reports `eth_blockNumber = 105` and valid receipt at block `103` -> `CrossCheckReceipt` returns Secondary's receipt and provider name.
- `TestRouter_CrossCheckBlockHash`: Primary reports mismatched block hash at height `102` while Secondary confirms `expectedHash` -> `CrossCheckBlockHash` detects the discrepancy cleanly.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./internal/blockchain/...`
Expected: FAIL with undefined `NewRouter`, `ManagedProvider`.

- [ ] **Step 3: Implement `internal/blockchain/router.go`**

- Sort providers by `Config.Priority` ascending.
- Helper `callWithFailover` iterates providers: acquires `Guard.Do`, calls `Client` method; on `ErrCircuitOpen` or `ClassifiedError` with `Retryable() == true`, continues to next provider; on non-retryable `ClassifiedError`, returns immediately; if all providers fail/open, returns `ErrNoHealthyProviders` joined with last error.
- `ReconcileTransaction`: queries healthy providers for `TransactionByHash`; returns `(tx, true, nil)` on first hit, `(ChainTransaction{}, false, nil)` if all healthy providers return `ErrTransactionNotFound`, or error if providers are unavailable.
- `CrossCheckReceipt`: checks `BlockNumber >= minBlock` on healthy providers before trusting `ErrReceiptNotFound`, returning the first valid receipt and its provider name.
- `CrossCheckBlockHash`: checks `HeaderByNumber` across healthy providers to confirm whether `expectedHash` is canonical on at least one healthy node.

- [ ] **Step 4: Run full verification suite**

Run: `go vet ./... && go test -race -count=1 ./...`
Expected: 0 vet warnings, all packages pass with `-race`.

- [ ] **Step 5: Commit**

```bash
git add internal/blockchain/router.go internal/blockchain/router_test.go
git commit -m "feat(blockchain): add multi-provider RPC failover router and cross-check verification"
```
