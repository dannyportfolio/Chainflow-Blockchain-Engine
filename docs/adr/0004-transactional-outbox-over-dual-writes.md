# ADR-0004: Transactional Outbox Pattern Over Dual Writes

## Status
Accepted (2026-11-21)

## Context
When `chainflow-api` accepts a transaction intent, it must both persist the intent in PostgreSQL and enqueue dispatch in NATS JetStream. Publishing to NATS before DB commit leaks phantom jobs on DB rollback; publishing after DB commit loses jobs if the API process crashes between commit and publish.

## Considered Options
1. **Best-effort dual write:** Commit PG, then publish to NATS synchronously.
2. **Transactional Outbox + Relay:** Insert `outbox_events` in the exact same PG transaction as `transaction_intents`, then publish asynchronously via an Outbox Relay using `FOR UPDATE SKIP LOCKED` (with short lock release before NATS I/O) and `Nats-Msg-Id` deduplication.

## Decision
Adopt **Transactional Outbox + Relay**.

## Trade-offs & Consequences
- **Positive:** Zero lost dispatches on API crash, atomic consistency between DB state and queued work, and clean recovery if NATS restarts.
- **Negative:** Adds 1-50 ms relay hop latency (configurable poll/notify interval) and requires outbox table retention pruning.
- **Trigger to Revisit:** Replace polling with PostgreSQL `LISTEN/NOTIFY` wakeups (as an optimization on top of the outbox table, never replacing it) or Debezium CDC if sub-5ms dispatch latency at >5k tx/s is needed.
