# 07 — Implementation Roadmap (TDD-Driven)

Each milestone produces verifiable, tested increments without speculative abstractions.

## Milestone M0 — Repository, Toolchain & Quality Gates
- **Objective:** Establish Go module, linter/vet targets, Makefile, migration harness, and CI skeleton.
- **Scope:** `go.mod`, `Makefile`, `.golangci.yml` (if tool installed / CI workflow), `.github/workflows/ci.yml`, `internal/config`.
- **Dependencies:** Architecture docs approved.
- **Test Strategy:** `go test ./...`, `go vet ./...`, config validation unit tests.
- **Definition of Done:** Empty skeleton compiles cleanly, CI workflow validates `gofmt`, `go vet`, `go test -race`.
- **Risks:** Missing local Docker/Foundry binaries on host; mitigate by supporting both pure-Go tests and Compose targets.

## Milestone M1 — Pure Domain & Explicit State Machine
- **Objective:** Implement core domain entities (`TransactionIntent`, `TransactionExecution`, `Wallet`, `NonceReservation`) and deterministic state machine rules.
- **Scope:** `internal/domain/` (zero external dependencies).
- **Dependencies:** M0.
- **Test Strategy (TDD):** Table-driven unit tests for every valid and invalid state transition, terminal state immutability, fee-cap guards, and RBF bump math.
- **Definition of Done:** 100% transition coverage on state machine matrix; zero external imports in `internal/domain`.
- **Risks:** State transition gaps; mitigated by verifying every row of `03-transaction-lifecycle.md` has a unit test.

## Milestone M2 — PostgreSQL Schema, Migrations & Persistence + Outbox
- **Objective:** Implement versioned SQL migrations, `pgx` repositories, optimistic locking, `wallet_nonces` locking, and transactional outbox.
- **Scope:** `migrations/`, `internal/persistence/`.
- **Dependencies:** M1.
- **Test Strategy:** Integration tests against real PostgreSQL (Compose / Testcontainers) verifying `UNIQUE(tenant_id, idempotency_key)`, concurrent lease contention, `SKIP LOCKED` outbox batching, and nonce allocation under `-race`.
- **Definition of Done:** All 16 concurrency/DB invariants covered by integration tests passing with `-race`.
- **Risks:** Docker availability on Windows host; verify Docker Engine access before running PG container tests.

## Milestone M3 — HTTP API Ingress & Idempotency
- **Objective:** Build Chi HTTP API (`POST /v1/transactions`, `GET /v1/transactions/{id}`, `POST /v1/transactions/{id}/cancel`, `/healthz`, `/readyz`, `/metrics`) with auth, rate limiting, and validation.
- **Scope:** `internal/security/` (auth, validation), `internal/application/`, `apps/api/`.
- **Dependencies:** M2.
- **Test Strategy:** HTTP handler unit tests + full DB-backed idempotency replay/conflict (`409`) tests + rate-limit tests.
- **Definition of Done:** API commits intent + outbox atomically; invalid payloads and bad signatures/addresses rejected at boundary.
- **Risks:** Payload canonicalization bugs on `request_hash`; test with deterministic canonical JSON/struct hashing.

## Milestone M4 — Isolated Signer, EVM Client & RPC Failover Router
- **Objective:** Implement `Signer` interface (dev/testnet ECDSA signer with zero key logging), `go-ethereum` adapter, EIP-1559 gas estimator, and multi-provider RPC router with circuit breakers.
- **Scope:** `internal/security/signer`, `internal/blockchain/`.
- **Dependencies:** M1.
- **Test Strategy:** Unit + integration tests against `go-ethereum/ethclient/simulated` AND fault-injecting mock JSON-RPC servers (timeouts, 429s, 503s, lagging blocks).
- **Definition of Done:** Precomputed `tx_hash` matches on-chain hash; provider failover and ambiguous timeout reconciliation verified.
- **Risks:** Subtle differences between `simulated.Backend` and live Sepolia RPC error strings; normalize RPC error classification carefully.

## Milestone M5 — NATS JetStream, Outbox Relay & Bounded Worker Pool
- **Objective:** Wire NATS JetStream streams, Outbox Relay, Lease/Expiry Sweeper, and pull-based bounded worker pool.
- **Scope:** `internal/messaging/`, `internal/concurrency/`, `apps/orchestrator/`, `apps/tx-worker/`.
- **Dependencies:** M2, M4.
- **Test Strategy:** Concurrency tests with `-race` (100 concurrent workers, duplicate NATS delivery, worker mid-flight cancel, graceful `SIGTERM` drain).
- **Definition of Done:** Zero data races, zero lost jobs across worker crashes and NATS restarts.
- **Risks:** NATS `AckWait` vs long RPC calls; ensure lease extension and `AckWait` margins align.

## Milestone M6 — Confirmation Monitor, RBF & Reorg Recovery
- **Objective:** Implement DB-driven confirmation monitor tracking confirmations, stale RBF fee bumps, mempool drops, external nonce replacement, and block-hash reorg recovery.
- **Scope:** `apps/confirmation-worker/`, `internal/blockchain/` reorg detector, `contracts/` minimal Foundry test contract.
- **Dependencies:** M5.
- **Test Strategy:** E2E tests against local simulated EVM / Anvil including `anvil_reorg` simulation, dropped tx simulation, and Sepolia testnet smoke verification.
- **Definition of Done:** Reorgs, drops, and RBF replacements recover cleanly without duplicate nonces or stuck intents.
- **Risks:** False reorg alerts from lagging secondary RPC; mitigate with multi-provider verification check.

## Milestone M7 — Observability, Docker Compose Stack, Chaos & Load Verification
- **Objective:** Complete Prometheus metrics, OpenTelemetry traces, `slog` structured logging, multi-stage non-root Dockerfiles, full `compose.yaml` (Prom/Grafana/Loki), and load/fault test suite.
- **Scope:** `internal/observability/`, `deployments/docker/`, `compose.yaml`, `tests/load/`, `tests/chaos/`.
- **Dependencies:** M6.
- **Test Strategy:** Execute 10,000-intent burst/sustained load test (100-worker target config) against mock RPC; inject DB/NATS/RPC faults under load; measure actual p95/p99 latencies and error rates.
- **Definition of Done:** Reproducible benchmark report with observed metrics, zero leaks, and green chaos suite.

## Milestone M8 — Kubernetes & Helm Migration Plan (Post-MVP)
- **Objective:** Package validated role binaries into Helm charts with HPA/KEDA queue-depth scaling, PodDisruptionBudgets, NetworkPolicies, and graceful preStop hooks.
- **Dependencies:** M7 local architecture validated.
