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


def write_csv(directory, csv_name, rows):
    csv_path = directory / csv_name

    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=SOURCE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def snapshot(table):
    with loader.engine.connect() as connection:
        rows = connection.execute(select(table)).mappings()
        return {row["order_id"]: dict(row) for row in rows}


def get_run(target_table, csv_name):
    """Đọc đúng nhật ký của bảng và file mẫu đang kiểm tra."""
    with loader.engine.connect() as connection:
        run_log = loader.get_pipeline_run_log_table(connection)

        statement = (
            select(run_log)
            .where(run_log.c.target_table == target_table)
            .where(run_log.c.source_file == csv_name)
        )

        return dict(connection.execute(statement).mappings().one())


def check_failed(target_table, csv_name, error_name):
    run = get_run(target_table, csv_name)

    assert run["status"] == "FAILED"
    assert run["finished_at"] is not None
    assert run["rows_written"] == 0
    assert run["rows_read"] is None
    assert run["rows_unchanged"] is None
    assert run["error_message"]
    assert error_name in run["error_message"]

    print(
        f"Đã đọc từ DB: status={run['status']}, "
        f"rows_written={run['rows_written']}, run_id={run['run_id']}"
    )


def main():
    table_name = "orders_runlog_test_" + uuid4().hex
    target_table = f"{loader.db_schema}.{table_name}"

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
        print(f"Bảng kiểm thử: {target_table}")

        with tempfile.TemporaryDirectory(prefix="olist_runlog_") as directory:
            directory = Path(directory)
            loader.RAW_DATA_DIR = directory

            # Test 1: Dữ liệu và nhật ký SUCCESS cùng được lưu.
            write_csv(directory, "initial.csv", [
                make_order("test_01", "shipped"),
                make_order("test_02", "delivered"),
            ])

            result = loader.run_orders_ingestion(
                "initial.csv", table_name, batch_size=1
            )

            baseline = snapshot(test_table)
            run = get_run(target_table, "initial.csv")

            assert len(baseline) == 2
            assert run["run_id"] == result["run_id"]
            assert run["status"] == "SUCCESS"
            assert run["rows_read"] == 2
            assert run["rows_written"] == 2
            assert run["rows_unchanged"] == 0
            assert run["finished_at"] is not None
            assert run["error_message"] is None

            print("PASS 1: Có dữ liệu và nhật ký SUCCESS đúng số dòng")

            # Test 2: Batch đầu UPDATE, batch sau bị từ chối.
            write_csv(directory, "rollback.csv", [
                make_order("test_01", "processing"),
                make_order("test_03", "__force_failure__"),
            ])

            try:
                loader.run_orders_ingestion(
                    "rollback.csv", table_name, batch_size=1
                )
            except IntegrityError:
                pass
            else:
                raise AssertionError("Phải phát sinh lỗi CHECK constraint")

            assert snapshot(test_table) == baseline
            check_failed(target_table, "rollback.csv", "IntegrityError")

            print("PASS 2: Dữ liệu rollback nhưng nhật ký FAILED vẫn còn")

            # Test 3: Cố ý không tạo missing.csv.
            try:
                loader.run_orders_ingestion(
                    "missing.csv", table_name, batch_size=1
                )
            except FileNotFoundError:
                pass
            else:
                raise AssertionError("Phải phát sinh FileNotFoundError")

            assert snapshot(test_table) == baseline
            check_failed(target_table, "missing.csv", "FileNotFoundError")

            print("PASS 3: Thiếu CSV vẫn lưu được FAILED và thông tin lỗi")

    finally:
        loader.RAW_DATA_DIR = original_data_dir

        if table_created:
            with loader.engine.begin() as connection:
                run_log = loader.get_pipeline_run_log_table(connection)

                result = connection.execute(
                    run_log.delete().where(
                        run_log.c.target_table == target_table
                    )
                )

                deleted_logs = result.rowcount
                test_table.drop(connection)

            print(f"Đã dọn bảng kiểm thử và {deleted_logs} nhật ký mẫu")

    print("Hoàn thành: 3/3 kiểm tra nhật ký thành công")


if __name__ == "__main__":
    main()