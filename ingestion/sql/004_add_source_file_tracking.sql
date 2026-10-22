ALTER TABLE ops.pipeline_run_log
    ADD COLUMN IF NOT EXISTS source_file_hash TEXT,
    ADD COLUMN IF NOT EXISTS file_skipped BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN ops.pipeline_run_log.source_file_hash IS
    'SHA-256 hash of the exact source file bytes used by this run.';

COMMENT ON COLUMN ops.pipeline_run_log.file_skipped IS
    'True when a previously successful identical input file was skipped.';

CREATE INDEX IF NOT EXISTS ix_pipeline_run_log_successful_file
    ON ops.pipeline_run_log (
        pipeline_name,
        target_table,
        source_file_hash
    )
    WHERE status = 'SUCCESS'
      AND source_file_hash IS NOT NULL;