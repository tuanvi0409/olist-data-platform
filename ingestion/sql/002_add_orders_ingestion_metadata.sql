ALTER TABLE raw.orders
    ADD COLUMN IF NOT EXISTS _row_hash TEXT,
    ADD COLUMN IF NOT EXISTS _loaded_at TIMESTAMPTZ;

COMMENT ON COLUMN raw.orders._row_hash IS
    'SHA-256 hash of source columns used to detect changed rows.';

COMMENT ON COLUMN raw.orders._loaded_at IS
    'UTC timestamp when the row was last inserted or updated by ingestion.';