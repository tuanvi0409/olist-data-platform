# Olist Data Platform

[![CI](https://github.com/tuanvi0409/olist-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/tuanvi0409/olist-data-platform/actions/workflows/ci.yml)

An end-to-end data engineering project that ingests Brazilian e-commerce data into PostgreSQL and transforms it with dbt into analytics-ready fact, dimension, and aggregate tables.

## Architecture

Olist CSV files → Python ingestion → PostgreSQL raw schema → dbt staging → intermediate models → fact and dimension tables → aggregate data marts.

### dbt Lineage

The lineage graph shows dependencies from raw sources through staging and intermediate transformations to fact, dimension, and aggregate models.

![dbt lineage](docs/images/dbt-lineage.png)

## Technology Stack

- Python, pandas, SQLAlchemy, and psycopg2
- PostgreSQL 16
- dbt Core
- Docker and Docker Compose
- Git, GitHub, and GitHub Actions

## Implemented Scope

- 9 raw source tables
- 9 staging models and 4 intermediate models
- 3 dimensions, 2 facts, and 3 aggregate marts
- 85 data quality tests: 82 generic and 3 business reconciliation tests
- dbt documentation and lineage
- Orders UPSERT with row-level change detection
- File-level content tracking and transactional pipeline run logging

## Ingestion Behavior

| Source | Loading strategy |
| --- | --- |
| orders | File tracking by SHA-256, then UPSERT by `order_id`; update only when the source row hash changes |
| Other 8 sources | Full refresh using pandas `to_sql` with `if_exists="replace"` |

The CLI requires an explicit `--table` argument. Using `--table all` processes all nine sources, including full refreshes of the other eight tables.

### Orders Validation and Row Change Detection

Before writing to PostgreSQL, the loader validates the CSV column structure and rejects empty datasets, NULL or blank order IDs, and duplicate order IDs.

Each order contains two ingestion metadata columns:

| Column | Purpose |
| --- | --- |
| `_row_hash` | SHA-256 hash of the source fields, used to detect changed rows |
| `_loaded_at` | Timestamp when ingestion last inserted or updated the row |

New orders are inserted. Existing orders are updated only when their hashes differ. Unchanged orders retain their data and metadata.

All batches within one orders load share a transaction. If a batch fails, earlier writes from that load are rolled back.

### Source File Tracking

Before parsing an orders CSV, ingestion reads the file once into an in-memory byte snapshot and calculates its SHA-256 hash. The same snapshot is used for both hashing and loading, so the tracked content always matches the processed content.

If the same file content has already completed successfully for the orders pipeline and target table, the loader records a successful skipped run and avoids CSV parsing and database UPSERT work. File identity is based on content, so renaming an unchanged file does not cause it to be processed again.

A PostgreSQL transaction-level advisory lock serializes logged orders ingestion runs for the same target. This prevents concurrent processes from both deciding that the same file is new.

This is file-level incremental processing. It does not yet implement source-side watermark extraction, change data capture, or streaming ingestion.

## Pipeline Run Log

Orders ingestion records each execution in `ops.pipeline_run_log`, including:

- Run ID, pipeline name, source file, and target table
- Status: `RUNNING`, `SUCCESS`, or `FAILED`
- Start and finish timestamps
- Source file hash and whether the file was skipped
- Row counts and error information

Successful runs report:

| Metric | Meaning |
| --- | --- |
| `rows_read` | Number of CSV rows parsed during the run |
| `rows_written` | Number of committed inserts plus updates |
| `rows_unchanged` | Number of parsed source rows that required no write |
| `file_skipped` | Whether previously successful identical content was skipped |

A skipped file reports zero for all three row counters because its CSV rows were not parsed during that run.

The initial `RUNNING` record is committed before ingestion starts.

Data changes and the `SUCCESS` update commit in the same transaction. When ingestion fails, data changes roll back and `FAILED` is recorded in a separate transaction.

For failed runs, `rows_written` is recorded as zero. Other counters remain NULL when they are not finalized.

## Schema Migrations

Versioned migrations are stored in `ingestion/sql`:

| Migration | Purpose |
| --- | --- |
| `001_add_orders_unique_constraint.sql` | Add the unique constraint on `raw.orders.order_id` |
| `002_add_orders_ingestion_metadata.sql` | Add `_row_hash` and `_loaded_at` |
| `003_create_pipeline_run_log.sql` | Create `ops.pipeline_run_log` |
| `004_add_source_file_tracking.sql` | Add source file hash, skipped-run tracking, and the successful-file lookup index |

## Local Development Workflow

Run commands from the project root with the virtual environment active.

The workflow below assumes that `.env` and the dbt connection profile are configured, the initial raw tables exist, and the schema migrations have been applied.

### Start PostgreSQL

```powershell
docker compose --env-file .env -f .\docker\docker-compose.yml up -d postgres
```

### Load Orders

```powershell
python .\ingestion\load_raw_data.py --table orders
```

The first run for previously untracked content reads the CSV and performs row-level change detection. A later run with identical file content is skipped before CSV parsing and reports:

```text
Bỏ qua file: nội dung đã được nạp thành công ở run <run_id>
Đã đọc: 0 dòng
Đã ghi (INSERT + UPDATE): 0 dòng
Không đổi: 0 dòng
```

### Load an Orders Batch CSV

To load an existing batch CSV:

```powershell
python .\ingestion\load_raw_data.py --table orders --csv-file .\data\raw\orders_batch.csv
```

The file must contain the same eight source columns as the original orders CSV.

- `--csv-file` is supported only with `--table orders`.
- Relative paths are resolved from the current working directory.
- Omitting `--csv-file` uses the original orders CSV.
- Orders absent from the batch remain in the database.
- Unseen file content is read in full and processed with the existing UPSERT and run logging.
- Previously successful identical content is skipped even if the file name changes.

To process a previously successful file again:

```powershell
python .\ingestion\load_raw_data.py --table orders --csv-file .\data\raw\orders_batch.csv --force
```

`--force` bypasses file-level skipping. Row-level hash comparison still prevents unnecessary updates to unchanged orders.

### Build and Test dbt Models

After ingestion succeeds:

```powershell
dbt build --project-dir .\dbt\olist_analytics
```

## Local Integration Verification

### Orders UPSERT

```powershell
python .\ingestion\verify_orders_upsert.py
```

Covers four scenarios:

1. Insert new orders with ingestion metadata.
2. Reload unchanged data without changing rows or metadata.
3. Update changed orders, insert new orders, and preserve unchanged orders.
4. Roll back earlier batches when a later batch fails.

### Pipeline Run Logging

```powershell
python .\ingestion\verify_pipeline_run_log.py
```

Covers three scenarios:

1. Commit data and a `SUCCESS` log with matching row counts.
2. Roll back data changes while preserving a `FAILED` log.
3. Record `FAILED` when the source CSV is missing.

### Orders File Tracking

```powershell
python .\ingestion\verify_orders_file_tracking.py
```

Covers five scenarios:

1. Process unseen file content and record its SHA-256 hash.
2. Skip identical content, including content under a different file name.
3. Process changed content and preserve orders absent from the batch.
4. Force a replay while row-level hashes prevent unnecessary writes.
5. Retry previously failed content after the failure cause is removed.

All three verification scripts use isolated test tables and clean up their fixtures.

## Verified Results

- Orders baseline: 99,441 rows and 99,441 distinct order IDs.
- Orders UPSERT verification: 4/4 checks passed.
- Pipeline run log verification: 3/3 checks passed.
- Orders file tracking verification: 5/5 checks passed.
- Replaying the same five-row sample records a skipped run without changing the orders baseline.
- dbt build: 21 models and 85 data tests completed successfully.
- dbt summary: PASS=106, WARN=0, ERROR=0, SKIP=0.
