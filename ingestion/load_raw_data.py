import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text


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


def create_raw_schema():
    """Tạo schema raw nếu schema chưa tồn tại."""
    with engine.begin() as connection:
        connection.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")
        )


def load_csv_to_postgres(csv_name, table_name):
    """Đọc một file CSV và nạp vào một bảng PostgreSQL."""
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


def main():
    create_raw_schema()

    for csv_name, table_name in csv_tables.items():
        load_csv_to_postgres(csv_name, table_name)

    print("\nĐã nạp thành công toàn bộ dữ liệu Olist!")


if __name__ == "__main__":
    main()