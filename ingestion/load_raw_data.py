import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

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
    """Đọc tham số dòng lệnh."""
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

    return parser.parse_args()


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


def upsert_orders(csv_name, table_name, batch_size=5000):
    """Thêm orders mới, chỉ cập nhật orders có nội dung thay đổi."""
    csv_path = RAW_DATA_DIR / csv_name

    if not csv_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

    print(f"Đang đọc {csv_name}...")

    # Raw orders đang lưu các cột nguồn dưới dạng text.
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

    # PostgreSQL sẽ nhận None thành NULL.
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
                # Tính hash trước khi thêm hai cột metadata.
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

    print(f"Đã đọc: {len(dataframe):,} dòng")
    print(f"Đã ghi (INSERT + UPDATE): {rows_written:,} dòng")
    print(f"Không đổi: {len(dataframe) - rows_written:,} dòng")


def main():
    args = parse_args()

    create_raw_schema()

    for csv_name, table_name in csv_tables.items():
        if args.table != "all" and table_name != args.table:
            continue

        if table_name == "orders":
            upsert_orders(csv_name, table_name)
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