# ADR-0008: Isolated Signer Interface and Dev/Prod Separation

## Status
Accepted (2026-11-21)

## Context
Transaction orchestration requires signing EIP-1559 payloads, which makes private keys the highest-impact asset in the system. Embedding `ecdsa.PrivateKey` handling inside workers or storing encrypted keys in the application database couples orchestration logic to key custody and widens blast radius.

## Considered Options
1. **Direct private key loading inside worker config:** Simple for demos, unsafe for production evolution and prone to accidental secret leakage.
2. **Isolated `Signer` Port + Environment-Restricted Dev Signer:** Define a narrow `Signer` interface (`SignEIP1559`, `Address`) that never exports key bytes, enforces `chain_id`/`from`/fee-cap checks, and ships initially with a testnet-only dev signer (`chain_id != 1` guard) replaceable by AWS KMS / Vault Transit / remote signer sidecar.

## Decision
Adopt **Isolated `Signer` Interface with Testnet-Guarded Dev Implementation**.

## Trade-offs & Consequences
- **Positive:** Workers never see raw private key bytes, zero secrets in DB/logs, and swapping to KMS/HSM in production requires implementing one adapter with zero orchestration changes.
- **Negative:** Local/testnet setups still inject a dev key via environment/secret mount (strictly guarded against mainnet `chain_id = 1`).
- **Trigger to Revisit:** Implement KMS/Vault remote signer adapter before any staging or mainnet deployment.
