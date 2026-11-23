# ADR-0001: Modular Monolith with Role-Scoped Entrypoints

## Status
Accepted (2026-11-21)

## Context
ChainFlow requires separate operational responsibilities (HTTP API ingress, outbox relay/sweeper, transaction execution workers, confirmation/reorg monitoring). Splitting into independent microservices with separate repositories or internal gRPC/REST calls at Day 0 adds distributed deployment complexity, schema coupling, and network latency before domain boundaries stabilize.

## Considered Options
1. **Separate Microservices per Role:** Separate Go modules/repos communicating over gRPC/HTTP + shared DB or DB-per-service.
2. **Single Monolithic Binary Only:** One process running HTTP, workers, and monitors together with no role separation.
3. **Modular Monolith with Role-Scoped Entrypoints:** Single Go module with strict `internal/` package boundaries, compiled into role binaries (`apps/api`, `apps/orchestrator`, `apps/tx-worker`, `apps/confirmation-worker`) plus a combined `cmd/chainflow-all` local runner.

## Decision
Adopt **Option 3: Modular Monolith with Role-Scoped Entrypoints**.

## Trade-offs & Consequences
- **Positive:** Zero internal RPC boilerplate, compile-time type safety across domain/application packages, independent horizontal scaling of `tx-worker` vs `api` in Compose/Kubernetes, and clean failure-domain isolation.
- **Negative:** All roles share one Go module and release version; database schema migrations must remain backward-compatible across rolling role restarts.
- **Trigger to Revisit:** Split a role into an independent service/repo if team ownership diverges or a single role (e.g., isolated remote Signer service) requires strict network/IAM segmentation.
