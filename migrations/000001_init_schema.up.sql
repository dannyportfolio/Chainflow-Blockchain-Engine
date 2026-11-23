-- ChainFlow Initial Schema (v1)
-- Follows PostgreSQL best practices: identity keys for append tables, UUIDv7 for external aggregates,
-- numeric(78,0) for uint256 wei values, timestamptz everywhere, and partial indexes for hot scans.

CREATE TABLE wallets (
    id                  uuid PRIMARY KEY,
    chain_id            bigint NOT NULL CHECK (chain_id > 0),
    address             bytea NOT NULL CHECK (octet_length(address) = 20),
    label               text NOT NULL,
    signer_key_ref      text NOT NULL,
    status              text NOT NULL CHECK (status IN ('ACTIVE', 'PAUSED', 'RETIRED')),
    max_inflight_nonces integer NOT NULL DEFAULT 1 CHECK (max_inflight_nonces BETWEEN 1 AND 64),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (chain_id, address)
);

CREATE TABLE wallet_nonces (
    chain_id            bigint NOT NULL,
    wallet_address      bytea NOT NULL CHECK (octet_length(wallet_address) = 20),
    next_nonce          bigint NOT NULL CHECK (next_nonce >= 0),
    last_synced_nonce   bigint NOT NULL DEFAULT 0 CHECK (last_synced_nonce >= 0),
    last_synced_at      timestamptz,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, wallet_address),
    FOREIGN KEY (chain_id, wallet_address) REFERENCES wallets (chain_id, address)
);

CREATE TABLE transaction_intents (
    id                      uuid PRIMARY KEY,
    tenant_id               text NOT NULL,
    idempotency_key         text NOT NULL,
    request_hash            bytea NOT NULL CHECK (octet_length(request_hash) = 32),

    chain_id                bigint NOT NULL CHECK (chain_id > 0),
    from_address            bytea NOT NULL CHECK (octet_length(from_address) = 20),
    to_address              bytea CHECK (to_address IS NULL OR octet_length(to_address) = 20),
    value_wei               numeric(78, 0) NOT NULL CHECK (value_wei >= 0),
    calldata                bytea NOT NULL DEFAULT '\x'::bytea,
    gas_limit_override      bigint CHECK (gas_limit_override IS NULL OR gas_limit_override >= 21000),
    max_fee_cap_wei         numeric(78, 0) NOT NULL CHECK (max_fee_cap_wei > 0),
    priority                smallint NOT NULL DEFAULT 0 CHECK (priority BETWEEN -10 AND 10),

    status                  text NOT NULL CHECK (status IN (
                                'CREATED', 'QUEUED', 'PROCESSING', 'PREPARED',
                                'BROADCASTED', 'CONFIRMING', 'RETRYING',
                                'FINALIZED', 'FAILED', 'CANCELLED', 'EXPIRED', 'REPLACED'
                            )),
    execution_outcome       text CHECK (execution_outcome IS NULL OR execution_outcome IN ('SUCCEEDED', 'REVERTED')),
    active_execution_id     uuid,
    attempt_count           integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts            integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    next_retry_at           timestamptz,
    expires_at              timestamptz NOT NULL,

    version                 bigint NOT NULL DEFAULT 1,
    lease_owner             text,
    lease_expires_at        timestamptz,
    last_error_code         text,
    last_error_message      text,
    trace_id                text,

    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    finalized_at            timestamptz,

    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (chain_id, from_address) REFERENCES wallets (chain_id, address)
);

CREATE TABLE transaction_executions (
    id                      uuid PRIMARY KEY,
    intent_id               uuid NOT NULL REFERENCES transaction_intents (id),
    attempt_number          integer NOT NULL CHECK (attempt_number > 0),
    chain_id                bigint NOT NULL,
    from_address            bytea NOT NULL CHECK (octet_length(from_address) = 20),
    nonce                   bigint NOT NULL CHECK (nonce >= 0),
    gas_limit               bigint NOT NULL CHECK (gas_limit >= 21000),
    max_priority_fee_wei    numeric(78, 0) NOT NULL CHECK (max_priority_fee_wei >= 0),
    max_fee_per_gas_wei     numeric(78, 0) NOT NULL CHECK (max_fee_per_gas_wei > 0),
    tx_hash                 bytea CHECK (tx_hash IS NULL OR octet_length(tx_hash) = 32),
    raw_signed_tx           bytea,
    status                  text NOT NULL CHECK (status IN (
                                'ALLOCATING_NONCE', 'SIGNED', 'BROADCASTED',
                                'CONFIRMING', 'CONFIRMED', 'DROPPED', 'REPLACED', 'REORGED', 'FAILED'
                            )),
    rpc_provider_used       text,
    broadcasted_at          timestamptz,
    error_code              text,
    error_message           text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (intent_id, attempt_number)
);

CREATE TABLE nonce_reservations (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id            bigint NOT NULL,
    wallet_address      bytea NOT NULL CHECK (octet_length(wallet_address) = 20),
    nonce               bigint NOT NULL CHECK (nonce >= 0),
    intent_id           uuid NOT NULL REFERENCES transaction_intents (id),
    status              text NOT NULL CHECK (status IN ('RESERVED', 'CONFIRMED', 'RELEASED', 'CONSUMED_EXTERNAL')),
    reserved_at         timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uidx_nonce_reservations_active
    ON nonce_reservations (chain_id, wallet_address, nonce)
    WHERE status = 'RESERVED';

CREATE INDEX idx_nonce_reservations_intent_id
    ON nonce_reservations (intent_id);

CREATE TABLE transaction_receipts (
    execution_id            uuid PRIMARY KEY REFERENCES transaction_executions (id),
    tx_hash                 bytea NOT NULL CHECK (octet_length(tx_hash) = 32),
    block_number            bigint NOT NULL CHECK (block_number >= 0),
    block_hash              bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    transaction_index       integer NOT NULL CHECK (transaction_index >= 0),
    gas_used                bigint NOT NULL CHECK (gas_used >= 0),
    effective_gas_price_wei numeric(78, 0) NOT NULL CHECK (effective_gas_price_wei >= 0),
    onchain_status          smallint NOT NULL CHECK (onchain_status IN (0, 1)),
    confirmations           integer NOT NULL DEFAULT 1 CHECK (confirmations >= 0),
    reorged                 boolean NOT NULL DEFAULT false,
    observed_at             timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            uuid NOT NULL UNIQUE,
    aggregate_type      text NOT NULL,
    aggregate_id        uuid NOT NULL,
    subject             text NOT NULL,
    event_version       smallint NOT NULL DEFAULT 1,
    payload             jsonb NOT NULL,
    trace_parent        text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    available_at        timestamptz NOT NULL DEFAULT now(),
    locked_until        timestamptz,
    published_at        timestamptz,
    publish_attempts    integer NOT NULL DEFAULT 0,
    last_error          text
);

CREATE TABLE transaction_state_events (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    intent_id           uuid NOT NULL REFERENCES transaction_intents (id),
    execution_id        uuid,
    from_status         text,
    to_status           text NOT NULL,
    actor               text NOT NULL,
    reason              text,
    metadata            jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);

-- Hot operational partial indexes
CREATE INDEX idx_outbox_unpublished
    ON outbox_events (available_at, id)
    WHERE published_at IS NULL;

CREATE INDEX idx_executions_active_monitor
    ON transaction_executions (chain_id, broadcasted_at)
    WHERE status IN ('BROADCASTED', 'CONFIRMING');

CREATE UNIQUE INDEX uidx_executions_tx_hash
    ON transaction_executions (chain_id, tx_hash)
    WHERE tx_hash IS NOT NULL;

CREATE INDEX idx_intents_active_leases
    ON transaction_intents (lease_expires_at)
    WHERE status IN ('PROCESSING', 'PREPARED') AND lease_expires_at IS NOT NULL;

CREATE INDEX idx_intents_retrying
    ON transaction_intents (next_retry_at)
    WHERE status = 'RETRYING';

CREATE INDEX idx_intents_expirable
    ON transaction_intents (expires_at)
    WHERE status IN ('CREATED', 'QUEUED', 'PROCESSING', 'PREPARED', 'RETRYING');

CREATE INDEX idx_executions_intent_id
    ON transaction_executions (intent_id);

CREATE INDEX idx_state_events_intent_id
    ON transaction_state_events (intent_id, id);

CREATE INDEX idx_intents_wallet_status
    ON transaction_intents (chain_id, from_address, status)
    WHERE status NOT IN ('FINALIZED', 'FAILED', 'CANCELLED', 'EXPIRED', 'REPLACED');
