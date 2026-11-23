# ADR-0003: PostgreSQL as Source of Truth and Short Transaction Boundaries

## Status
Accepted (2026-11-21)

## Context
Blockchain transaction orchestration involves multi-second external I/O (gas estimation, signing, RPC broadcast, block finality). Holding database locks or open SQL transactions across external RPC calls exhausts connection pools and causes deadlocks. Conversely, keeping authoritative state in NATS or Redis risks state loss or split-brain on restart.

## Considered Options
1. **Redis + Broker State:** Store active tx state in Redis/NATS and write asynchronously to SQL.
2. **PostgreSQL as Single Source of Truth with Short Transactions:** Persist every intent, execution, nonce reservation, and transition in PostgreSQL (`pgx`) using `<10 ms` transactions and optimistic version checks (`version`).

## Decision
Adopt **PostgreSQL as Single Source of Truth** with strict short-transaction boundaries (never hold a DB transaction across RPC, Signer, or NATS network calls).

## Trade-offs & Consequences
- **Positive:** ACID durability, relational integrity between intents/executions/nonces, partial indexes for hot active rows, and deterministic crash recovery.
- **Negative:** PostgreSQL write IOPS becomes the primary vertical scaling ceiling for a single cluster.
- **Trigger to Revisit:** Introduce Redis only for read-heavy caching/rate-limiting if read QPS saturates PG connections, or partition high-volume append tables by `created_at` past 10M rows.
