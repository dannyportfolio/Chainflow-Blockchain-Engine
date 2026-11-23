# Domain & Configuration Notes

## `internal/config/config.go`
- `SecretString`: Implements `String()`, `GoString()`, and `slog.LogValuer` returning `[REDACTED]` for non-empty values; `UnsafeValue()` is the explicit boundary accessor for drivers and key loaders.
- `AckWait > LeaseTTL`: `Config.Validate()` enforces `NATS.AckWait > Worker.LeaseTTL` so JetStream does not redeliver an in-flight message while a worker's database lease is still valid.

## `internal/domain`
- `minRBFBumpBps = 1000`: Ethereum mempool nodes (including Geth) require at least a 10% fee bump for Replace-By-Fee on the same nonce; `CalculateRBFBump` enforces `>= 1000 bps` and at least `+1 wei` while clamping under `maxFeeCap`.
- `TransitionIntent`: Enforces active lease ownership (`LeaseOwner` match while `now < LeaseExpiresAt`), terminal-state immutability, expiration before entering `PROCESSING`, `tx_hash` + `ExecutionID` persistence before entering `PREPARED`, and `AttemptCount < MaxAttempts` before entering `RETRYING`.
