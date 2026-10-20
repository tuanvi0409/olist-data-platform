import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
from uuid import uuid4
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert


# Xác định thư mục gốc của project
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Đọc cấu hình kết nối từ file .env
load_dotenv(PROJECT_ROOT / ".env", override=True)

db_schema = os.getenv("DB_SCHEMA", "raw")

database_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME"),
)

engine = create_engine(database_url)

# Ánh xạ tên file CSV thành tên bảng PostgreSQL
csv_tables = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
}


def parse_args():
    """Đọc và kiểm tra tham số dòng lệnh."""
    parser = argparse.ArgumentParser(
        description="Nạp dữ liệu Olist vào PostgreSQL raw layer."
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=["all", *csv_tables.values()],
        help=(
            "Chọn một bảng để nạp riêng hoặc dùng 'all' "
            "để nạp toàn bộ dữ liệu."
        ),
    )

    parser.add_argument(
        "--csv-file",
        help=(
            "Đường dẫn CSV riêng cho orders. Đường dẫn tương đối "
            "tính từ thư mục đang chạy lệnh; chỉ dùng với --table orders."
        ),
    )

    args = parser.parse_args()

    if args.csv_file is not None:
        if args.table != "orders":
            parser.error("--csv-file chỉ dùng với --table orders")

        if not args.csv_file.strip():
            parser.error("--csv-file không được để trống")

    return args


def create_raw_schema():
    """Tạo schema raw nếu schema chưa tồn tại."""
    with engine.begin() as connection:
        connection.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")
        )


def load_csv_to_postgres(csv_name, table_name):
    """Full refresh một file CSV vào PostgreSQL."""
    csv_path = RAW_DATA_DIR / csv_name

    if not csv_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

    print(f"Đang đọc {csv_name}...")

    dataframe = pd.read_csv(csv_path)

    print(
        f"Đang nạp {len(dataframe):,} dòng "
        f"vào {db_schema}.{table_name}..."
    )

    dataframe.to_sql(
        name=table_name,
        con=engine,
        schema=db_schema,
        if_exists="replace",
        index=False,
        chunksize=5000,
    )

    print(f"Hoàn thành {db_schema}.{table_name}")


def calculate_row_hash(record):
    """Tính SHA-256 từ dictionary chỉ chứa các cột nguồn."""
    payload = json.dumps(
        record,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()



def get_pipeline_run_log_table(connection):
    """Đọc cấu trúc bảng nhật ký bằng connection đang có."""
    return Table(
        "pipeline_run_log",
        MetaData(),
        schema="ops",
        autoload_with=connection,
    )


def start_pipeline_run(csv_name, table_name):
    """Tạo và commit bản ghi RUNNING trước khi ingestion bắt đầu."""
    run_id = uuid4()

    with engine.begin() as connection:
        run_log = get_pipeline_run_log_table(connection)

        connection.execute(
            run_log.insert().values(
                run_id=run_id,
                pipeline_name="raw_orders_ingestion",
                source_file=csv_name,
                target_table=f"{db_schema}.{table_name}",
                status="RUNNING",
                started_at=datetime.now(timezone.utc),
            )
        )

    return run_id


def finish_pipeline_run(
    connection, run_id, status, metrics=None, error_message=None
):
    """Kết thúc lần chạy trong transaction do bên gọi quản lý."""
    if status not in ("SUCCESS", "FAILED"):
        raise ValueError("Trạng thái kết thúc phải là SUCCESS hoặc FAILED")

    metrics = metrics or {}
    run_log = get_pipeline_run_log_table(connection)

    result = connection.execute(
        run_log.update()
        .where(run_log.c.run_id == run_id)
        .where(run_log.c.status == "RUNNING")
        .values(
            status=status,
            finished_at=datetime.now(timezone.utc),
            rows_read=metrics.get("rows_read"),
            rows_written=metrics.get("rows_written"),
            rows_unchanged=metrics.get("rows_unchanged"),
            error_message=error_message,
        )
    )

    if result.rowcount != 1:
        raise RuntimeError(
            f"Không tìm thấy run RUNNING để cập nhật: {run_id}"
        )


def upsert_orders(csv_name, table_name, batch_size=5000, run_id=None):
    """UPSERT theo hash; ghi SUCCESS cùng transaction nếu có run_id."""
    if batch_size <= 0:
        raise ValueError("batch_size phải lớn hơn 0")

    csv_path = RAW_DATA_DIR / csv_name

    if not csv_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

    print(f"Đang đọc {csv_name}...")
    dataframe = pd.read_csv(csv_path, dtype=str)

    source_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    missing = sorted(set(source_columns) - set(dataframe.columns))
    unexpected = sorted(set(dataframe.columns) - set(source_columns))

    if missing or unexpected:
        raise ValueError(
            f"CSV orders sai cấu trúc: thiếu={missing}, dư={unexpected}"
        )

    if dataframe.empty:
        raise ValueError("CSV orders không có dòng dữ liệu")

    if dataframe["order_id"].isna().any():
        raise ValueError("orders có order_id NULL")

    if dataframe["order_id"].str.strip().eq("").any():
        raise ValueError("orders có order_id chỉ chứa khoảng trắng")

    if dataframe["order_id"].duplicated().any():
        raise ValueError("orders có order_id bị trùng")

    dataframe = dataframe.astype(object).where(
        pd.notna(dataframe),
        None,
    )

    orders_table = Table(
        table_name,
        MetaData(),
        schema=db_schema,
        autoload_with=engine,
    )

    update_columns = [
        column
        for column in source_columns
        if column != "order_id"
    ] + ["_row_hash", "_loaded_at"]

    rows_written = 0
    loaded_at = datetime.now(timezone.utc)

    print(
        f"Đang UPSERT {len(dataframe):,} dòng "
        f"vào {db_schema}.{table_name}..."
    )

    with engine.begin() as connection:
        for start in range(0, len(dataframe), batch_size):
            batch = dataframe.iloc[
                start:start + batch_size
            ].to_dict(orient="records")

            for record in batch:
                record["_row_hash"] = calculate_row_hash(record)
                record["_loaded_at"] = loaded_at

            statement = pg_insert(orders_table).values(batch)

            statement = statement.on_conflict_do_update(
                index_elements=["order_id"],
                set_={
                    column: statement.excluded[column]
                    for column in update_columns
                },
                where=orders_table.c["_row_hash"].is_distinct_from(
                    statement.excluded["_row_hash"]
                ),
            ).returning(orders_table.c.order_id)

            result = connection.execute(statement)
            rows_written += len(result.fetchall())

        metrics = {
            "rows_read": len(dataframe),
            "rows_written": rows_written,
            "rows_unchanged": len(dataframe) - rows_written,
        }

        # Cùng connection: dữ liệu và SUCCESS commit cùng nhau.
        if run_id is not None:
            finish_pipeline_run(
                connection,
                run_id,
                "SUCCESS",
                metrics,
            )

    print(f"Đã đọc: {metrics['rows_read']:,} dòng")
    print(f"Đã ghi (INSERT + UPDATE): {metrics['rows_written']:,} dòng")
    print(f"Không đổi: {metrics['rows_unchanged']:,} dòng")

    return metrics


def run_orders_ingestion(csv_name, table_name, batch_size=5000):
    """Quản lý nhật ký và lỗi của một lần ingestion orders."""
    run_id = start_pipeline_run(csv_name, table_name)
    print(f"Run ID: {run_id}")

    try:
        metrics = upsert_orders(
            csv_name,
            table_name,
            batch_size=batch_size,
            run_id=run_id,
        )

    except Exception as error:
        # Nếu lỗi xảy ra khi ghi dữ liệu, transaction đã rollback.
        # Ghi FAILED trong một transaction riêng.
        try:
            with engine.begin() as connection:
                finish_pipeline_run(
                    connection,
                    run_id,
                    "FAILED",
                    metrics={"rows_written": 0},
                    error_message=(
                        f"{type(error).__name__}: {error}"
                    )[:4000],
                )

        except Exception as log_error:
            print(
                f"Không ghi được FAILED cho run {run_id}: {log_error}",
                file=sys.stderr,
            )

        # Giữ lỗi gốc để terminal báo thất bại.
        raise

    print(f"Đã ghi nhật ký SUCCESS: {run_id}")

    return {"run_id": run_id, **metrics}

def main():
    args = parse_args()

    create_raw_schema()

    for csv_name, table_name in csv_tables.items():
        if args.table != "all" and table_name != args.table:
            continue

        if table_name == "orders":
            source_file = csv_name

            if args.csv_file is not None:
                source_file = str(Path(args.csv_file).resolve())

            run_orders_ingestion(source_file, table_name)
        else:
            load_csv_to_postgres(csv_name, table_name)

    if args.table == "all":
        print("\nĐã nạp thành công toàn bộ dữ liệu Olist!")
    else:
        print(
            f"\nĐã nạp thành công bảng "
            f"{db_schema}.{args.table}!"
        )


if __name__ == "__main__":
    main()