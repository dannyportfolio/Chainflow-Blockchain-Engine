# Milestone 6: Confirmation Monitor, RBF & Reorg Recovery Implementation Plan

> **Goal:** Implement DB-driven transaction confirmation tracking, Replace-By-Fee (RBF) fee bumping for stuck/dropped transactions, external nonce replacement detection, and canonical block-hash reorg recovery, verified against simulated EVM / Anvil.

## Architecture & Execution Flow

### End-to-End Monitor Loop (`apps/confirmation-worker`)

```
[Poll Tick (e.g. 2s)]
        │
        ▼
1. Fetch head = RPC.BlockNumber()
2. Fetch active executions = ExecutionRepo.ListActiveExecutions(chainID, batchSize)
   (ExecutionStatus in: BROADCASTED, CONFIRMING)
        │
        ├──► ExecutionStatus == BROADCASTED (awaiting inclusion)
        │       ├── RPC.TransactionReceipt(txHash) found:
        │       │     ├── UpsertReceipt(...)
        │       │     ├── UpdateExecutionStatus(CONFIRMING)
        │       │     └── TransitionIntent(BROADCASTED -> CONFIRMING)
        │       └── Receipt NOT found:
        │             ├── Check RPC.LatestNonceAt(from) > exec.Nonce:
        │             │     └── Confirm txHash absent -> External Replacement:
        │             │         ├── UpdateExecutionStatus(REPLACED)
        │             │         ├── NonceCoordinator.ConfirmNonce(...)
        │             │         └── TransitionIntent(BROADCASTED -> REPLACED)
        │             └── Else check stale/dropped:
        │                   ├── RPC.TransactionByHash(txHash) == NotFound OR age > StalePendingTTL:
        │                   │     ├── UpdateExecutionStatus(DROPPED / REPLACED)
        │                   │     └── TransitionIntent(BROADCASTED -> RETRYING, reason=STALE_PENDING/DROPPED)
        │                   │         + OutboxEvent(tx.retry) -> tx-worker re-executes with SAME nonce + RBF bump (>=15%)
        │                   └── Otherwise keep BROADCASTED
        │
        └──► ExecutionStatus == CONFIRMING (mined, awaiting finality)
                ├── Verify canonical block: header = RPC.HeaderByNumber(receipt.BlockNumber)
                ├── IF header.Hash != receipt.BlockHash (REORG DETECTED):
                │     ├── UpdateExecutionStatus(REORGED)
                │     ├── Check RPC.TransactionReceipt(txHash) on new fork:
                │     │     ├── Mined in new block -> UpsertReceipt(new) + UpdateExecutionStatus(CONFIRMING)
                │     │     └── Not mined -> Check TransactionByHash:
                │     │           ├── Pending in mempool -> UpdateExecutionStatus(BROADCASTED) + TransitionIntent(CONFIRMING -> BROADCASTED)
                │     │           └── Evicted -> TransitionIntent(CONFIRMING -> RETRYING, reason=REORGED) + OutboxEvent
                └── ELSE (canonical hash matches):
                      ├── confirmations = head - receipt.BlockNumber + 1
                      └── IF confirmations >= RequiredConfirmations:
                            ├── UpdateExecutionStatus(CONFIRMED)
                            ├── NonceCoordinator.ConfirmNonce(chainID, from, exec.Nonce)
                            └── TransitionIntent(CONFIRMING -> FINALIZED, outcome=SUCCEEDED|REVERTED)
```

---

## Step-by-Step Implementation Plan (TDD)

### Task 1: Config & Persistence Read Extensions
- **Files:**
  - Modify: `internal/config/config.go`, `internal/config/config_test.go`
  - Modify: `internal/application/ports.go`
  - Modify: `internal/persistence/memory/store.go`
  - Modify: `internal/persistence/postgres/execution_repo.go`
  - Modify: `internal/persistence/storetest/contract.go`
- **Changes:**
  1. Add `MonitorConfig` to `config.Config` (`PollInterval`, `BatchSize`, `StalePendingTTL`, `RBFBumpBps` defaulting to `1500` = 15% >= geth's 10% rule).
  2. Add `GetReceiptByExecutionID(ctx, execID string) (domain.TransactionReceipt, error)` to `ExecutionRepository` (needed by the monitor when checking `CONFIRMING` executions against `HeaderByNumber`).
  3. Implement `GetReceiptByExecutionID` in both `memory` and `postgres` repositories and cover it in `storetest/contract.go`.

### Task 2: Reorg Verification & Multi-Provider Guard (`internal/blockchain`)
- **Files:**
  - Create: `internal/blockchain/reorg.go`
  - Create: `internal/blockchain/reorg_test.go`
- **Changes:**
  1. Implement `ReorgVerifier` that checks `HeaderByNumber(blockNumber)` across configured providers (or router) to distinguish a true canonical reorg from a lagging secondary RPC.
  2. Unit tests with mock `Client`s covering: matching hash, true reorg, and lagging provider disagreement.

### Task 3: RBF Fee Bump & Retry Integration (`apps/tx-worker` / `internal/application`)
- **Files:**
  - Modify: `internal/application/` & `apps/tx-worker/main.go` (or execution service)
- **Changes:**
  1. When `tx-worker` picks up an intent in `RETRYING` that already has a prior `PREPARED`/`BROADCASTED`/`REORGED` execution with a reserved nonce:
     - **Reuse the existing reserved nonce** (never allocate a new nonce for an RBF or reorg retry of an already-nonced intent).
     - Bump `MaxPriorityFeePerGas` and `MaxFeePerGas` by `max(suggested, prior * (1 + RBFBumpBps/10000))`, capped at `intent.MaxFeeCapWei`.
     - Create a new `TransactionExecution` (`AttemptNumber + 1`), sign, persist, and broadcast.
  2. Unit tests verifying nonce reuse and >= 15% fee bump math within `MaxFeeCapWei`.

### Task 4: Confirmation Monitor Service (`internal/application/confirmation_monitor.go`)
- **Files:**
  - Create: `internal/application/confirmation_monitor.go`
  - Create: `internal/application/confirmation_monitor_test.go`
- **Changes:**
  1. Implement `ConfirmationMonitor` with `RunOnce(ctx)` and `Run(ctx)` polling loop.
  2. Handle all 5 monitor outcomes cleanly:
     - Inclusion (`BROADCASTED -> CONFIRMING`, receipt persisted, `status == 0` vs `1` recorded).
     - Finality (`CONFIRMING -> FINALIZED` with `SUCCEEDED` or `REVERTED`, `ConfirmNonce` called).
     - Stale pending / mempool drop (`BROADCASTED -> RETRYING` + outbox publish).
     - External nonce replacement (`BROADCASTED -> REPLACED` + `ConfirmNonce`).
     - Chain reorg (`CONFIRMING -> CONFIRMING` on new block, `BROADCASTED` if back in mempool, or `RETRYING` if evicted).
  3. Table-driven unit tests using `memory.Store` + mock `blockchain.Client` covering every transition above, including reverted-on-chain tx finalizing as `FINALIZED` (`REVERTED`).

### Task 5: `confirmation-worker` Entrypoint (`apps/confirmation-worker/main.go`)
- **Files:**
  - Create: `apps/confirmation-worker/main.go`
- **Changes:**
  1. Wire config, PostgreSQL pool/store, `blockchain.Router`, `ConfirmationMonitor`, and graceful `SIGTERM`/`SIGINT` shutdown (matching `apps/orchestrator/main.go` and `apps/tx-worker/main.go`).

### Task 6: Minimal Foundry Contract & E2E / Anvil Reorg Verification
- **Files:**
  - Create: `contracts/src/Counter.sol` (or minimal test contract) + `contracts/foundry.toml` (if Foundry is used)
  - Create: `tests/e2e/confirmation_reorg_test.go` (or build-tagged integration test)
- **Changes:**
  1. Test against `go-ethereum/ethclient/simulated` and/or Anvil (`anvil_reorg`, dropped tx, RBF replacement).
  2. Run `go test -race ./...` and `go vet ./...` to verify zero races and clean build.
