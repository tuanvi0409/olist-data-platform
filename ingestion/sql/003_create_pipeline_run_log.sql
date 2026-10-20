CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
    run_id UUID PRIMARY KEY,

    pipeline_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    target_table TEXT NOT NULL,

    status TEXT NOT NULL DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,

    rows_read BIGINT CHECK (rows_read >= 0),
    rows_written BIGINT CHECK (rows_written >= 0),
    rows_unchanged BIGINT CHECK (rows_unchanged >= 0),

    error_message TEXT
);

COMMENT ON TABLE ops.pipeline_run_log IS
    'Execution history of ingestion runs. One row represents one run.';

COMMENT ON COLUMN ops.pipeline_run_log.rows_written IS
    'Number of rows committed by ingestion: INSERT plus UPDATE.';