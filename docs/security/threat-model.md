# Security Threat Model (STRIDE) & Signer Isolation

## 1. Trust Boundaries & Data Flow

```text
[Untrusted Caller]
       |  (Boundary 1: Public/Partner HTTPS API - AuthN, AuthZ, Rate Limit, Schema Validation)
       v
[ChainFlow API & Orchestration Core]  <--->  [PostgreSQL / NATS] (Boundary 2: Internal Network / Credentials)
       |
       +---> (Boundary 3: Isolated Signer Boundary - Strict Tx Intent Check, Zero Key Export)
       |
       +---> (Boundary 4: Untrusted External EVM RPC Providers - TLS, Response Verification, Caps)
```

## 2. Critical Assets

| Asset | Sensitivity | Impact if Compromised |
| :--- | :--- | :--- |
| **Wallet Private Keys / KMS Credentials** | Critical | Direct theft or unauthorized signing of on-chain assets. |
| **API Keys / Tenant Secrets** | High | Unauthorized transaction submission draining wallet gas/funds. |
| **PostgreSQL Transaction & Nonce State** | High | Double-execution, nonce corruption, denial of service. |
| **RPC Provider API Keys** | Medium | Quota exhaustion, rate-limit DoS, billing abuse. |
| **Audit Logs & Telemetry** | Medium | Repudiation failure or secret leakage if carelessly logged. |

## 3. STRIDE Threat Analysis & Mitigations

| ID | Category | Threat | Target | Impact | Likelihood | Concrete Mitigation |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **S1** | Spoofing | Stolen or forged API credential submits fraudulent transactions. | API Ingress | Critical | Medium | Store only SHA-256/Argon2id-hashed API keys in config/DB; require TLS; bind each key to allowed `tenant_id` and `from_address` allowlist. |
| **S2** | Spoofing | Malicious/compromised RPC node returns spoofed receipt or wrong chain data. | RPC Router | High | Low | Verify `eth_chainId` on provider startup/health check; verify `tx_hash` of fetched receipt matches signed `tx_hash`; cross-check anomalies across providers. |
| **T1** | Tampering | Replay or mutation of an intent using the same `Idempotency-Key` with altered `to`/`value`/`data`. | API Idempotency | Critical | Medium | Compute SHA-256 `request_hash` over canonical payload; reject key reuse with different `request_hash` (`409 Conflict`). |
| **T2** | Tampering | Worker or DB bug modifies `to`, `value`, `data`, or `chain_id` between API acceptance and signing. | Signer Boundary | Critical | Low | `Signer` receives immutable intent fields + explicit `chain_id` + `nonce`, enforces EIP-155/EIP-1559 replay protection, and validates `from` matches key address. |
| **T3** | Tampering | SQL injection via caller-supplied metadata or filters. | Persistence | Critical | Low | Parameterized queries strictly via `pgx` (`$1, $2`); zero string interpolation in SQL. |
| **R1** | Repudiation | Caller denies submitting an intent, or operator cannot trace who triggered a cancellation/RBF. | Audit Trail | Medium | Medium | Append-only `transaction_state_events` recording `actor`, `tenant_id`, `request_id`, `reason`, and `created_at`. |
| **I1** | Info Disclosure | Private keys, seed phrases, RPC API keys, or auth headers leaked into logs, metrics, or error responses. | Observability | Critical | Medium | Custom `slog` redaction / typed secret wrapper (`SecretString` with `LogValue() = "[REDACTED]"`), sanitized HTTP errors, never log `raw_signed_tx` or key material. |
| **I2** | Info Disclosure | Tenant A reads Tenant B's transaction intent via UUID guessing (IDOR). | `GET /v1/transactions/{id}` | High | Medium | Every query scopes strictly by `WHERE id = $1 AND tenant_id = $2` derived from authenticated principal. |
| **D1** | Denial of Service | Unbounded intent submission floods PG/NATS or spikes gas fees to drain wallet ETH. | API / Gas Estimator | High | High | Per-tenant token-bucket rate limits, payload size caps (`64 KB`), strict `max_fee_cap_wei` & `gas_limit` caps, and bounded worker pools. |
| **E1** | Elevation | Compromised worker container reads private keys from disk/env or escalates host privileges. | Container / Signer | Critical | Low | Multi-stage scratch/distroless non-root containers, read-only rootfs, drop Linux capabilities, and isolated `Signer` interface ready for out-of-process KMS/HSM. |

## 4. Isolated Signer Architecture Contract

All signing flows through a narrow domain boundary:

```go
type SignRequest struct {
    ChainID              *big.Int
    From                 common.Address
    To                   *common.Address
    Nonce                uint64
    ValueWei             *big.Int
    Data                 []byte
    GasLimit             uint64
    MaxPriorityFeePerGas *big.Int
    MaxFeePerGas         *big.Int
}

type Signer interface {
    SignEIP1559(ctx context.Context, req SignRequest) (*types.Transaction, error)
    Address(ctx context.Context, keyRef string) (common.Address, error)
}
```

**Security Invariants on `Signer`:**
1. **No Key Export:** The interface never exposes `GetPrivateKey`, bytes, or ECDSA key structs outside `internal/security/signer`.
2. **Domain Guardrails Inside Signer:** Before signing, the signer verifies `req.ChainID > 0`, `req.From` matches the resolved `keyRef` address, and `req.MaxFeePerGas <= configuredHardCap`.
3. **Environment Separation:** Production/staging deployments must swap the local testnet dev-signer (`EnvKeystoreSigner`, restricted to non-mainnet chain IDs!) for a remote KMS/HSM implementation (AWS KMS / Vault Transit / dedicated signer sidecar) without changing any orchestration or worker code.
4. **Dev Safety Guard:** The local dev-signer explicitly **refuses** to sign for Ethereum Mainnet (`chain_id == 1`) unless an explicit override flag is set, preventing accidental mainnet execution during testing.
