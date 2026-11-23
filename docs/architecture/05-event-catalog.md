# 05 — Event Catalog & NATS JetStream Topology

## 1. JetStream Stream Topology

| Stream | Subjects | Retention | Storage | Replicas | Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `CHAINFLOW_TX` | `chainflow.v1.tx.>` | `WorkQueuePolicy` (or `LimitsPolicy` with `MaxAge: 7d`) | File | 1 (Dev) / 3 (Prod) | Primary durable command & lifecycle event stream; deduplicated via `Nats-Msg-Id` (`DedupWindow: 24h`). |
| `CHAINFLOW_DLQ` | `chainflow.v1.dlq.>` | `LimitsPolicy` (`MaxAge: 30d`) | File | 1 (Dev) / 3 (Prod) | Poison-pill / max-deliveries-exhausted events for inspection & manual replay. |

*Choice of `WorkQueuePolicy` vs `LimitsPolicy`:* Use `LimitsPolicy` with durable queue-group consumers per role if multiple independent services (e.g., future webhook notifier + tx-worker) subscribe to different subjects, or `WorkQueuePolicy` for strict single-consumption command subjects (`chainflow.v1.tx.dispatch`, `chainflow.v1.tx.retry`). Let's separate command subjects (`CHAINFLOW_COMMANDS`, `WorkQueuePolicy`) from lifecycle domain events (`CHAINFLOW_EVENTS`, `LimitsPolicy`, 7d retention) if downstream consumers are added later; for MVP, a single `CHAINFLOW_TX` stream with `LimitsPolicy` + explicit durable consumers per consumer group provides clean replay and observability without premature stream proliferation.

## 2. Versioned Event Catalog (`v1`)

All envelopes share standard headers:
- `Nats-Msg-Id`: Outbox `event_id` (UUIDv7) for JetStream server-side deduplication.
- `traceparent`: W3C Trace Context header for end-to-end OpenTelemetry propagation.
- `schema_version`: `"1"`.

### 2.1 `chainflow.v1.tx.dispatch`
- **Version:** `v1`
- **Producer:** API Ingress (via `outbox_events`)
- **Consumer:** `tx-worker` (Durable pull consumer: `tx-worker-dispatch`)
- **Delivery Guarantee:** At-least-once; `AckWait: 45s`, `MaxDeliver: 5`
- **Idempotency Behavior:** Consumer attempts atomic DB lease `WHERE status IN ('QUEUED', 'RETRYING')`. If already `PROCESSING`/`BROADCASTED`/`FINALIZED`, consumer logs duplicate delivery and `Ack()`s immediately.
- **Retry Policy:** On transient infra error before lease, `NakWithDelay(backoff)`. After `MaxDeliver`, JetStream advisory routes to `CHAINFLOW_DLQ` (`chainflow.v1.dlq.tx.dispatch`).
- **Ordering:** No strict global ordering required; per-wallet serialization is enforced at the PostgreSQL `wallet_nonces` lock, not by blocking the whole JetStream partition.
- **Payload:**
```json
{
  "event_id": "01934a1b-...",
  "event_type": "tx.dispatch.v1",
  "occurred_at": "2026-11-21T08:00:00Z",
  "intent_id": "01934a1a-...",
  "tenant_id": "tenant_alpha",
  "chain_id": 11155111,
  "from_address": "0x...",
  "priority": 0
}
```

### 2.2 `chainflow.v1.tx.retry`
- **Version:** `v1`
- **Producer:** Confirmation Monitor (on drop/reorg/stale RBF) or Lease Sweeper (on expired worker lease) via `outbox_events`
- **Consumer:** `tx-worker` (Durable pull consumer: `tx-worker-retry`, or shared dispatch consumer)
- **Delivery Guarantee:** At-least-once; `AckWait: 45s`, `MaxDeliver: 5`
- **Idempotency Behavior:** Checks `intent.version >= expected_min_version` and `intent.status == 'RETRYING'` before claiming lease. Also carries `retry_reason` (`LEASE_EXPIRED`, `RPC_TIMEOUT_RECONCILE`, `MEMPOOL_DROPPED`, `STALE_RBF`, `REORGED`) so the worker knows whether to reconcile `tx_hash` first or prepare a bumped replacement.
- **Payload:**
```json
{
  "event_id": "01934a2c-...",
  "event_type": "tx.retry.v1",
  "occurred_at": "2026-11-21T08:02:00Z",
  "intent_id": "01934a1a-...",
  "attempt_number": 2,
  "retry_reason": "STALE_RBF",
  "previous_execution_id": "01934a1b-...",
  "chain_id": 11155111
}
```

### 2.3 `chainflow.v1.tx.status_changed`
- **Version:** `v1`
- **Producer:** Any state owner (Worker, Confirmation Monitor, API Cancel, Expiry Sweeper) via `outbox_events`
- **Consumer:** Observability / Audit / Future Webhook Dispatcher (optional in MVP; stored in stream for replay/testing)
- **Delivery Guarantee:** At-least-once; `LimitsPolicy`
- **Idempotency Behavior:** Keyed by `(intent_id, new_version)` so downstream observers ignore out-of-order stale events (`version < last_seen_version`).
- **Ordering:** Causal ordering via monotonically increasing `intent_version`.
- **Payload:**
```json
{
  "event_id": "01934a3f-...",
  "event_type": "tx.status_changed.v1",
  "occurred_at": "2026-11-21T08:03:12Z",
  "intent_id": "01934a1a-...",
  "intent_version": 5,
  "from_status": "CONFIRMING",
  "to_status": "FINALIZED",
  "execution_outcome": "SUCCEEDED",
  "tx_hash": "0x...",
  "block_number": 7123456
}
```

### 2.4 `chainflow.v1.dlq.tx`
- **Version:** `v1`
- **Producer:** Consumer error boundary / JetStream MaxDeliver advisory
- **Consumer:** Operator inspection / replay CLI (future)
- **Payload:** Original event envelope + `failure_reason`, `deliveries_attempted`, `last_worker_id`, `failed_at`.
