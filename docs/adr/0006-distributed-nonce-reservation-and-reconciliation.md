# ADR-0006: Distributed Nonce Reservation and Chain Reconciliation

## Status
Accepted (2026-11-21)

## Context
EVM accounts require strictly sequential nonces (`0, 1, 2, ...`). If multiple worker goroutines or worker replicas concurrently query `eth_getTransactionCount("pending")` and sign transactions for the same wallet, they will collide on the same nonce or create permanent nonce gaps when an earlier tx fails before broadcast.

## Considered Options
1. **In-memory `sync.Mutex` per wallet:** Breaks across multiple worker replicas and loses state on process restart.
2. **Stateless RPC `pending` polling per job:** Race condition under concurrent workers and stale RPC views.
3. **PostgreSQL Row-Locked Nonce Table (`wallet_nonces`) + `nonce_reservations` + Chain Reconciliation:** Serialize nonce assignment per `(chain_id, wallet_address)` inside a `<5 ms` PG transaction, enforce `max_inflight_nonces` (`1` default in MVP Phase 1, configurable up to `16`), persist signed `tx_hash` before broadcast, and reconcile against RPC `pending` nonce on errors/restarts.

## Decision
Adopt **PostgreSQL-Coordinated Nonce Reservation with On-Chain Reconciliation**.

## Trade-offs & Consequences
- **Positive:** Safe across multiple worker replicas, survives crashes without losing reserved nonces, prevents nonce gaps on pre-broadcast crashes, and supports RBF fee bumps reusing the exact same `nonce`.
- **Negative:** Single hot wallet throughput is bounded by `max_inflight_nonces` and short PG row-lock serialization.
- **Trigger to Revisit:** Implement multi-wallet pool sharding or Redis-assisted windowed nonce leases if a single wallet requires >100 tx/s sustained pipelining.
