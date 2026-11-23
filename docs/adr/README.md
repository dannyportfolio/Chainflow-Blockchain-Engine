# Architecture Decision Records (ADRs)

This directory records the foundational architectural decisions for ChainFlow.

## Index

| ADR | Title | Status | Date |
| :--- | :--- | :--- | :--- |
| [0001](0001-modular-monolith-with-role-entrypoints.md) | Modular Monolith with Role-Scoped Entrypoints | Accepted | 2026-11-21 |
| [0002](0002-nats-jetstream-for-async-orchestration.md) | NATS JetStream for Durable Async Dispatch | Accepted | 2026-11-21 |
| [0003](0003-postgresql-source-of-truth-and-tx-boundaries.md) | PostgreSQL as Source of Truth and Short TX Boundaries | Accepted | 2026-11-21 |
| [0004](0004-transactional-outbox-over-dual-writes.md) | Transactional Outbox Pattern Over Dual Writes | Accepted | 2026-11-21 |
| [0005](0005-bounded-worker-pool-and-backpressure.md) | Pull-Based Bounded Worker Pool and Backpressure | Accepted | 2026-11-21 |
| [0006](0006-distributed-nonce-reservation-and-reconciliation.md) | Distributed Nonce Reservation and Chain Reconciliation | Accepted | 2026-11-21 |
| [0007](0007-multi-provider-evm-rpc-failover-and-hedging.md) | Multi-Provider EVM RPC Failover and Circuit Breaking | Accepted | 2026-11-21 |
| [0008](0008-isolated-signer-boundary.md) | Isolated Signer Interface and Dev/Prod Separation | Accepted | 2026-11-21 |
| [0009](0009-compose-first-then-kubernetes.md) | Docker Compose Local Validation Before Kubernetes | Accepted | 2026-11-21 |
