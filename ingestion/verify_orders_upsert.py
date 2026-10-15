import csv
import tempfile
from pathlib import Path
from uuid import uuid4

from sqlalchemy import (
    CheckConstraint, Column, DateTime, MetaData, Table,
    Text, UniqueConstraint, select,
)
from sqlalchemy.exc import IntegrityError

import load_raw_data as loader


SOURCE_COLUMNS = [
    "order_id", "customer_id", "order_status",
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def make_order(order_id, status):
    """Tạo một đơn hàng mẫu có đủ tám cột nguồn."""
    return {
        "order_id": order_id,
        "customer_id": "customer_test",
        "order_status": status,
        "order_purchase_timestamp": "2018-01-01 10:00:00",
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": "2018-01-10 00:00:00",
    }


def run_load(directory, table_name, rows):
    """Ghi CSV mẫu rồi gọi chính hàm ingestion đang sử dụng."""
    csv_path = directory / "orders_test.csv"

    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=SOURCE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    loader.upsert_orders(csv_path.name, table_name, batch_size=1)


def snapshot(table):
    """Đọc trạng thái bảng để so sánh trước và sau."""
    with loader.engine.connect() as connection:
        result = connection.execute(select(table)).mappings()
        return {row["order_id"]: dict(row) for row in result}


def main():
    table_name = "orders_upsert_test_" + uuid4().hex

    test_table = Table(
        table_name,
        MetaData(),
        *(Column(name, Text) for name in SOURCE_COLUMNS),
        Column("_row_hash", Text),
        Column("_loaded_at", DateTime(timezone=True)),
        UniqueConstraint("order_id"),
        CheckConstraint("order_status <> '__force_failure__'"),
        schema=loader.db_schema,
    )

    original_data_dir = loader.RAW_DATA_DIR
    table_created = False

    try:
        test_table.create(loader.engine)
        table_created = True
        print(f"Bảng kiểm thử: {loader.db_schema}.{table_name}")

        with tempfile.TemporaryDirectory(prefix="olist_upsert_") as directory:
            directory = Path(directory)
            loader.RAW_DATA_DIR = directory

            # Test 1: Thêm hai đơn mới.
            rows = [
                make_order("test_01", "shipped"),
                make_order("test_02", "delivered"),
            ]

            run_load(directory, table_name, rows)
            first = snapshot(test_table)

            assert len(first) == 2
            assert all(
                row["_row_hash"] and row["_loaded_at"]
                for row in first.values()
            )
            print("PASS 1: INSERT hai đơn mới, có đủ metadata")

            # Test 2: Chạy lại cùng dữ liệu.
            run_load(directory, table_name, rows)

            assert snapshot(test_table) == first
            print("PASS 2: Chạy lại giữ nguyên dữ liệu và metadata")

            # Test 3: Đổi một đơn và thêm một đơn.
            rows[0]["order_status"] = "delivered"
            rows.append(make_order("test_03", "created"))

            run_load(directory, table_name, rows)
            changed = snapshot(test_table)

            assert len(changed) == 3
            assert changed["test_01"]["order_status"] == "delivered"
            assert (
                changed["test_01"]["_row_hash"]
                != first["test_01"]["_row_hash"]
            )
            assert (
                changed["test_01"]["_loaded_at"]
                != first["test_01"]["_loaded_at"]
            )
            assert changed["test_02"] == first["test_02"]
            assert changed["test_03"]["_row_hash"]
            assert changed["test_03"]["_loaded_at"]

            print(
                "PASS 3: UPDATE đơn đổi, INSERT đơn mới, "
                "giữ nguyên đơn không đổi"
            )

            # Test 4: Batch đầu UPDATE, batch sau cố ý gây lỗi.
            failing_rows = [
                make_order("test_01", "processing"),
                make_order("test_04", "__force_failure__"),
            ]

            try:
                run_load(directory, table_name, failing_rows)
            except IntegrityError:
                pass
            else:
                raise AssertionError(
                    "Database phải từ chối trạng thái kiểm thử"
                )

            assert snapshot(test_table) == changed
            print(
                "PASS 4: Batch sau lỗi, "
                "thay đổi ở batch trước cũng rollback"
            )

    finally:
        loader.RAW_DATA_DIR = original_data_dir

        if table_created:
            test_table.drop(loader.engine)
            print("Đã xóa bảng kiểm thử")

    print("Hoàn thành: 4/4 kiểm tra thành công")


if __name__ == "__main__":
    main()