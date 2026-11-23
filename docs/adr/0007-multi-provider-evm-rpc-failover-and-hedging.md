# ADR-0007: Multi-Provider EVM RPC Failover and Circuit Breaking

## Status
Accepted (2026-11-21)

## Context
Public and managed EVM RPC providers suffer transient 429 rate limits, 5xx outages, timeouts, and occasional stale/lagging block heads. Relying on a single RPC endpoint creates a hard single point of failure and risks false reorg/drop conclusions on stale responses.

## Considered Options
1. **Single `ethclient.Client` endpoint:** Simple, zero failover.
2. **Multi-provider RPC Router with Circuit Breakers, Per-Provider Semaphores, and Cross-Check Verification:** Maintain prioritized RPC providers; track sliding-window error rates/latencies; trip circuit breaker on unhealthy providers; failover cleanly; and cross-check suspected reorgs/missing receipts against a secondary healthy provider.

## Decision
Adopt **Multi-Provider EVM RPC Failover Router** behind a narrow `blockchain.Client` interface.

## Trade-offs & Consequences
- **Positive:** Eliminates single-provider downtime, mitigates rate-limit saturation, and prevents false reorgs from one lagging node.
- **Negative:** Requires managing provider priority/rate budgets and normalizing provider-specific JSON-RPC error strings (`nonce too low`, `replacement transaction underpriced`, `already known`).
- **Trigger to Revisit:** Add hedged read requests (parallel `eth_getTransactionReceipt` to 2 providers) if tail read latency exceeds confirmation SLA.
