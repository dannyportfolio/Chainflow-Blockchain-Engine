# 01 — Requirements Analysis & Scope Boundaries

## 1. Functional Requirements (MVP)

| ID | Requirement | Priority | Acceptance Anchor |
| :--- | :--- | :--- | :--- |
| FR-01 | Submit EVM transaction intent (`chain_id`, `from`, `to`, `value`, `data`, `gas_limit_override`, `priority`, `idempotency_key`, `expires_at`) via HTTP API. | Must | Validated payload persisted atomically with outbox event; `202 Accepted` returns `intent_id` and initial state. |
| FR-02 | Enforce idempotent intent creation per `(tenant_id, idempotency_key)`. | Must | Identical replay returns original `intent_id` (`200`/`202`); mismatched payload with same key returns `409 Conflict`. |
| FR-03 | Query transaction intent status, active execution attempt, `tx_hash`, confirmation count, and append-only state transition history. | Must | `GET /v1/transactions/{id}` returns authoritative DB state and audit trail. |
| FR-04 | Safe cancellation of intents before on-chain broadcast (or fee-bump zero-value cancel when policy allows). | Must | `POST /v1/transactions/{id}/cancel` transitions `CREATED`/`QUEUED`/`RETRYING` to `CANCELLED`; rejects terminal states. |
| FR-05 | Atomic transactional outbox publishing to NATS JetStream. | Must | Outbox publisher relays committed events with fencing/locking and marks `published_at` idempotently. |
| FR-06 | Bounded worker pool consuming execution jobs with backpressure, timeouts, and processing leases. | Must | No unbounded goroutines; NATS pull batching respects worker semaphore capacity. |
| FR-07 | Distributed per-wallet nonce allocation and pending-nonce reconciliation. | Must | Concurrent requests for the same `(chain_id, wallet_address)` never receive duplicate nonces across replicas. |
| FR-08 | Gas estimation and EIP-1559 fee calculation with hard safety caps (`max_fee_per_gas_cap`). | Must | Rejects or stalls transactions exceeding configured gas/fee caps rather than draining wallet funds. |
| FR-09 | Isolated signing via `Signer` interface (dev/testnet ECDSA signer initially; KMS/HSM replaceable). | Must | Signer verifies `chain_id`, `from`, and nonce; private keys never touch DB or logs. |
| FR-10 | Multi-provider EVM RPC failover with circuit breaking and deterministic `tx_hash` persistence before broadcast. | Must | Precomputes `tx_hash` from signed tx, records it in DB, then broadcasts; fails over on 5xx/timeout/429. |
| FR-11 | Confirmation monitor tracking block finality, dropped mempool txs, RBF replacements, and chain reorganizations. | Must | Tracks confirmations to `required_confirmations`; marks `REORGED` and re-queues/reconciles if block hash changes. |
| FR-12 | Configurable retry policy with exponential backoff + jitter and Dead-Letter routing after `max_attempts`. | Must | Transient RPC failures retry cleanly; terminal errors move intent to `FAILED` and route message to DLQ. |

## 2. Non-Functional Requirements

| Category | Target / Constraint | Notes |
| :--- | :--- | :--- |
| **Durability** | Zero silent loss of acknowledged intents (`202 Accepted`). | Synchronous PostgreSQL commit (`intent` + `outbox_event`) before API response. |
| **Delivery Semantics** | At-least-once event delivery; idempotent state & chain reconciliation. | Exactly-once RPC broadcast is impossible; prevented via precomputed `tx_hash` + nonce uniqueness. |
| **Concurrency Safety** | Zero data races under `go test -race`; strict upper bounds on goroutines, DB conns, and RPC concurrency. | Verified on all worker, nonce, and outbox packages. |
| **Availability & Failover** | Tolerate single worker crash, NATS restart, or primary RPC outage without manual DB repair. | Lease expiry sweeper + JetStream redelivery + RPC health-scored failover. |
| **Observability** | Prometheus `/metrics`, OpenTelemetry trace propagation (`traceparent`), structured JSON logs with correlation IDs, `/healthz` & `/readyz`. | No secrets, raw private keys, or full signed payloads in telemetry. |
| **Security** | Input validation at ingress, API-key auth per tenant, TLS-ready RPC clients, non-root containers, least-privilege DB user. | Threat-modeled via STRIDE (`docs/security/threat-model.md`). |

## 3. Load & Performance Evaluation Targets (Experimental Benchmarks, Not Guarantees)

- **Target Test Envelope (Mock/Local RPC):** 10,000 transaction intents submitted across burst and sustained profiles, with `100` max workers as an initial experimental configuration.
- **Separation of Concerns:** Benchmark application throughput against a controllable mock/simulated RPC first to measure DB, outbox, NATS, and Go worker pool bottlenecks separately from public Sepolia RPC rate limits.
- **API Latency Goal (Local/Mock):** p99 intent acceptance (`POST /v1/transactions`) < 50 ms under nominal DB pool load (excluding client network latency).

## 4. Scope Boundaries: MVP vs Deferred

| Capability | MVP (Included) | Deferred (Post-MVP) |
| :--- | :--- | :--- |
| **Networks** | Single active testnet (Ethereum Sepolia, `11155111`) + local simulated EVM (`go-ethereum` simulated backend / Anvil). | Multi-chain simultaneous routing (Arbitrum, Optimism, Base, Polygon) with chain-specific finality rules. |
| **Signing** | Isolated `Signer` interface + encrypted/env-injected testnet dev signer for local/Sepolia testing only. | AWS KMS / GCP KMS / HashiCorp Vault Transit / Fireblocks / MPC remote signer implementations. |
| **Authentication** | Hashed API key (`X-API-Key` / Bearer) + tenant scoping + token-bucket rate limit. | OAuth2/OIDC, mTLS client certificates, fine-grained RBAC UI. |
| **Messaging & State** | PostgreSQL + NATS JetStream + Transactional Outbox. | Redis cache/rate-limiter (only if read QPS or distributed rate-limiting bottlenecks PG). |
| **Orchestration Runtime** | Docker Compose stack (API, Outbox/Orchestrator, Workers, Confirmation Monitor, PG, NATS, Prometheus, Grafana, Loki). | Production Kubernetes + Helm + KEDA/HPA queue-depth autoscaling + multi-region active-active. |
| **Blockchain Features** | EIP-1559 Type 2 txs, ABI calldata passthrough, gas estimation, RBF fee bumping, reorg detection. | Account Abstraction (ERC-4337 bundlers), Flashbots/MEV-protected relays, Blob txs (EIP-4844), cross-chain bridging. |
| **UI** | None (API + Grafana dashboards). | Operator web console for manual replay/DLQ inspection. |

## 5. Risks & Mitigations Register

1. **Tooling environment gap (`docker`, `forge`/`anvil` absent from Windows host PATH):** Mitigate by designing pure-Go `go-ethereum/ethclient/simulated` integration tests alongside Docker Compose manifests so core concurrency, DB, and EVM logic remain testable immediately while Compose/WSL2/Docker access is confirmed.
2. **Hot-wallet nonce contention:** Mitigate with short PG row locks on `wallet_nonces`, separate `nonce_reservations` state, and bounded per-wallet in-flight windows.
3. **Out-of-band wallet usage:** If an external actor uses the same testnet private key outside ChainFlow, local nonce tracking drifts. Mitigate via periodic + error-triggered `eth_getTransactionCount("pending")` reconciliation and alerting.

## 6. Explicit Assumptions

- **[A1]** ChainFlow operates dedicated orchestration wallets (do not share managed hot wallets with external scripts).
- **[A2]** MVP callers provide pre-encoded calldata (`to`, `value`, `data`) or contract call parameters validated at ingress; ChainFlow does not execute arbitrary off-chain business scripts.
- **[A3]** Finality for Sepolia MVP is defined by configurable confirmation depth (`default: 6` blocks for testnet dev/test, `12+` configurable; mainnet-grade `finalized` tag support designed into the RPC interface).
