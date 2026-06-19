-- Tokens: metadata, one row per (chain, token)
CREATE TABLE IF NOT EXISTS tokens (
    chain        TEXT    NOT NULL,
    address      TEXT    NOT NULL,
    name         TEXT,
    symbol       TEXT,
    decimals     INT,
    total_supply NUMERIC,
    PRIMARY KEY (chain, address)
);

-- Snapshots: the completion ledger. id == job_id (uuid).
CREATE TABLE IF NOT EXISTS snapshots (
    id           UUID        PRIMARY KEY,
    chain        TEXT        NOT NULL,
    token        TEXT        NOT NULL,
    from_block   BIGINT,                         -- set at completion (min chunk_from)
    block_number BIGINT,                         -- set at completion (max chunk_to)
    state        TEXT        NOT NULL DEFAULT 'scanning', -- scanning|ready|error
    chunks_total BIGINT      NOT NULL DEFAULT 0,
    done_seen    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_snapshots_token ON snapshots (chain, token, block_number);

-- Chunk ledger: makes chunks_done idempotent. Derived count, never a counter.
CREATE TABLE IF NOT EXISTS snapshot_chunks (
    snapshot_id UUID   NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    chunk_from  BIGINT NOT NULL,
    chunk_to    BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, chunk_from, chunk_to)
);

-- Transfers: immutable chain facts, shared across snapshots of the same token.
-- NOT keyed by snapshot — a transfer is the same row for every snapshot.
CREATE TABLE IF NOT EXISTS transfers (
    chain        TEXT    NOT NULL,
    token        TEXT    NOT NULL,
    tx_hash      TEXT    NOT NULL,
    log_index    INT     NOT NULL,
    block_number BIGINT  NOT NULL,
    from_addr    TEXT    NOT NULL,
    to_addr      TEXT    NOT NULL,
    value        NUMERIC NOT NULL,
    PRIMARY KEY (chain, token, tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_transfers_scan ON transfers (chain, token, block_number);

-- Balances: per-snapshot holder balances.
CREATE TABLE IF NOT EXISTS balances (
    snapshot_id UUID    NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    address     TEXT    NOT NULL,
    balance     NUMERIC NOT NULL,
    last_block  BIGINT  NOT NULL,
    PRIMARY KEY (snapshot_id, address)
);
CREATE INDEX IF NOT EXISTS idx_balances_snapshot ON balances (snapshot_id);

-- Coverage: single contiguous interval of scanned blocks per (chain, token).
-- Used by future phases to skip rescanning. Conservatively under-claims on
-- disjoint scans (see repo AdvanceCoverage).
CREATE TABLE IF NOT EXISTS token_coverage (
    chain        TEXT   NOT NULL,
    token        TEXT   NOT NULL,
    covered_from BIGINT NOT NULL,
    covered_to   BIGINT NOT NULL,
    PRIMARY KEY (chain, token)
);