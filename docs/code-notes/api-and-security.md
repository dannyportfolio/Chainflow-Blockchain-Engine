# API, Auth & Idempotency Notes

## `internal/application/intent_service.go`
- Canonical `RequestHash`: SHA-256 digest computed over fixed binary/length-prefixed encoding of `(chain_id, from, to, value_wei, calldata, gas_limit_override, max_fee_cap_wei, priority)` so JSON whitespace or field ordering differences never cause false idempotency conflicts, while any semantic field change triggers `ErrIdempotencyConflict` (`409`).
- UUIDv7 generation: Generates time-ordered RFC 9562 v7 UUIDs using `time.Now()` + `crypto/rand` to avoid random UUIDv4 B-tree fragmentation in PostgreSQL primary keys.

## `internal/security`
- `APIKeyAuthenticator`: Hashes configured raw or pre-hashed SHA-256 API keys and keys lookup on `[32]byte` SHA-256 digests so raw credentials never persist in memory maps or logs.
- `TenantRateLimiter`: Token bucket per tenant using an injected clock function for deterministic testing.

## `internal/api`
- Enforces `64 KiB` request body ceiling (`http.MaxBytesReader`), strict `json.Decoder.DisallowUnknownFields()`, required `Idempotency-Key` header, `X-Request-ID` propagation, panic recovery, and sanitized HTTP error JSON responses.
