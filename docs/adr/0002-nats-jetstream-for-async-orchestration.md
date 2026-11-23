# ADR-0002: NATS JetStream for Durable Async Dispatch

## Status
Accepted (2026-11-21)

## Context
ChainFlow needs durable, at-least-once asynchronous job delivery with pull-based backpressure, deduplication headers, configurable `AckWait`/`MaxDeliver`, and dead-letter routing.

## Considered Options
1. **PostgreSQL `SKIP LOCKED` polling only:** Workers poll PostgreSQL directly for `QUEUED` jobs.
2. **Apache Kafka:** High-throughput partitioned commit log.
3. **Redis Streams:** Lightweight in-memory/AOF stream broker.
4. **NATS JetStream:** Lightweight Go-native persistent broker with pull consumers, `Nats-Msg-Id` deduplication, and File storage.

## Decision
Adopt **NATS JetStream** fed by a PostgreSQL transactional outbox.

## Trade-offs & Consequences
- **Positive:** Low operational footprint (~tens of MB RAM locally vs JVM/ZooKeeper/KRaft), native Go client, explicit `Ack`/`NakWithDelay` pull semantics that map directly to worker pool capacity, and built-in deduplication window.
- **Negative:** Adds one stateful messaging component to Docker Compose/K8s; requires managing stream limits and DLQ advisories.
- **Trigger to Revisit:** Switch to Kafka if multi-TB long-term event retention (>100k events/s) or enterprise Kafka ecosystem integration becomes mandatory.
