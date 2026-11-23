# ChainFlow Project Constitution

## 1. Purpose
ChainFlow is a fault-tolerant, event-driven transaction orchestration engine for EVM-compatible blockchains. It guarantees durable intent capture, explicit lifecycle orchestration, safe nonce coordination, isolated signing, and deterministic finality reconciliation.

## 2. Non-Negotiable Architectural Principles

1. **PostgreSQL is the Single Source of Truth:** Messaging (NATS JetStream) provides durable delivery and backpressure, never authoritative business state.
2. **At-Least-Once Processing + Idempotency:** Every API write, outbox publish, consumer handler, and RPC reconciliation path MUST tolerate duplicate delivery without duplicate on-chain execution.
3. **Explicit State Machines:** State transitions are centralized in domain transition logic with optimistic version checks (`version`) and lease ownership. Arbitrary column updates are forbidden.
4. **Transactional Outbox:** Intent creation and state-driven dispatch events MUST commit atomically in the same PostgreSQL transaction.
5. **Bounded Concurrency:** Every worker pool, internal queue, DB pool, per-wallet execution, and per-RPC-provider concurrency MUST have explicit configured bounds and backpressure behavior.
6. **Context Propagation & Graceful Shutdown:** `context.Context` flows from ingress through DB, broker, workers, and RPC calls. Shutdown drains in-flight work before closing DB and broker connections.
7. **Security & Key Isolation:** Private keys never enter logs, metrics, traces, or database rows. Signing occurs strictly behind an isolated `Signer` boundary.
8. **Evidence Over Claims:** No feature or reliability property is complete without failing-first tests, `-race` verification where concurrent, and explicit failure-injection coverage.

## 3. Architectural Constraints

- **Language:** Go (`go1.27+` toolchain verified; idiomatic standard library + minimal justified dependencies).
- **Database Access:** SQL-first via `pgx` and versioned SQL migrations.
- **No Premature Microservices:** Start as a modular monolith (`internal/` packages with strict dependency direction) with role-scoped entrypoints.
- **No Production Funds in Dev/Test:** Development and automated verification use local simulated EVM chains and Sepolia testnet only.
