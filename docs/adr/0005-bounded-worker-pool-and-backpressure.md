# ADR-0005: Pull-Based Bounded Worker Pool and Backpressure

## Status
Accepted (2026-11-21)

## Context
Unbounded goroutine-per-message consumers exhaust memory, open too many PostgreSQL connections, and trigger RPC 429 rate limits under traffic bursts. Push-based queues also risk `AckWait` expiration while jobs sit queued in process memory.

## Considered Options
1. **Push consumer + per-message goroutine:** Fast to write, unbounded memory/goroutines under load.
2. **Pull consumer + fixed worker pool + semaphore:** Pull from JetStream strictly up to `freeWorkerSlots`, process via fixed `N` worker goroutines with per-provider RPC semaphores and token buckets.

## Decision
Adopt **Pull-Based Bounded Worker Pool** with explicit capacity limits (`WORKER_CONCURRENCY`, `PG_MAX_CONNS`, `RPC_MAX_INFLIGHT`).

## Trade-offs & Consequences
- **Positive:** Predictable memory/CPU footprint, natural backpressure kept inside JetStream's durable queue, zero `AckWait` starvation in Go heap, and clean `SIGTERM` drain.
- **Negative:** Requires tuning `fetchBatchSize` and `AckWait` relative to worst-case RPC latency.
- **Trigger to Revisit:** Add dynamic concurrency adjustment (AIMD / queue-depth scaling) after baseline 10,000-intent fixed-pool benchmarks are measured in Milestone M7.
