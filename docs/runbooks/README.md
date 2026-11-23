# Operational Runbooks

Runbooks will be authored alongside implementation milestones (M2-M7):
- Stuck `PREPARED` / `PROCESSING` lease recovery & manual inspection
- Wallet nonce drift reconciliation (`eth_getTransactionCount` vs `wallet_nonces`)
- NATS JetStream DLQ inspection & safe intent replay
- Primary EVM RPC provider outage & manual priority override
- Chain reorganization incident verification
