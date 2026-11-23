# Persistence & Contract Test Notes

## `migrations/embed.go`
- Embeds all `*.sql` migration scripts (`//go:embed *.sql`) and enforces contiguous version numbering (`1, 2, ...`) with paired `.up.sql` and `.down.sql` scripts.

## `internal/persistence/postgres`
- `BuildPoolConfig`: Injects `statement_timeout = 5000` (5 seconds) on every connection so runaway queries or locked rows cannot stall pool capacity across network calls.
- `Migrate`: Acquires PostgreSQL session advisory lock `884201001` before inspecting `schema_migrations`, ensuring safe concurrent startup across replicas.

## `internal/persistence/memory`
- Test-only reference store implementing `internal/application` ports in memory for deterministic unit and concurrency testing when PostgreSQL is unavailable. Must never be wired in production binaries.

## `internal/persistence/storetest/contract.go`
- `intent_idempotency_and_tenant_isolation`: Verifies initial creation (`Created=true`), identical payload replay returning original `intent_id` (`Created=false`), mutated payload returning `ErrIdempotencyConflict`, and tenant scoping on `GetIntentByID`.
- `optimistic_locking_and_outbox_lifecycle`: Verifies outbox lease exclusivity during `leaseDuration`, reclaim after lease expiration, exclusion after `MarkPublished`, optimistic version increments, and `ErrVersionConflict` on stale updates.
- `nonce_reservation_inflight_limit_and_reconciliation`: Verifies sequential nonce allocation, `ErrWalletInflightLimit` backpressure when `MaxInflightNonces` is saturated, slot release on `ConfirmNonce`, and `ReconcilePendingNonce` advancing `next_nonce` and marking superseded reservations `CONSUMED_EXTERNAL`.
