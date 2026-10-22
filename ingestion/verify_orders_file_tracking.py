import csv
import hashlib
import tempfile
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import DropConstraint

import load_raw_data as loader


SOURCE_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
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
    path = directory / csv_name
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=SOURCE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(table):
    with loader.engine.connect() as connection:
        rows = connection.execute(select(table)).mappings()
        return {row["order_id"]: dict(row) for row in rows}


def get_run(run_id):
    with loader.engine.connect() as connection:
        run_log = loader.get_pipeline_run_log_table(connection)
        statement = select(run_log).where(run_log.c.run_id == run_id)
        return dict(connection.execute(statement).mappings().one())


def main():
    identifier = uuid4().hex
    table_name = "orders_file_test_" + identifier
    target_table = f"{loader.db_schema}.{table_name}"
    status_check = CheckConstraint(
        "order_status <> '__force_failure__'",
        name="ck_file_status_" + identifier,
    )
    test_table = Table(
        table_name,
        MetaData(),
        *(Column(name, Text) for name in SOURCE_COLUMNS),
        Column("_row_hash", Text),
        Column("_loaded_at", DateTime(timezone=True)),
        UniqueConstraint("order_id"),
        status_check,
        schema=loader.db_schema,
    )
    original_data_dir = loader.RAW_DATA_DIR
    table_created = False

    try:
        test_table.create(loader.engine)
        table_created = True
        print(f"Bảng kiểm thử: {target_table}")

        with tempfile.TemporaryDirectory(prefix="olist_files_") as directory:
            directory = Path(directory)
            loader.RAW_DATA_DIR = directory

            initial_hash = write_csv(directory, "batch.csv", [
                make_order("test_01", "shipped"),
                make_order("test_02", "delivered"),
            ])
            result = loader.run_orders_ingestion("batch.csv", table_name)
            run = get_run(result["run_id"])
            baseline = snapshot(test_table)
            assert len(baseline) == 2
            assert run["status"] == "SUCCESS"
            assert run["source_file_hash"] == initial_hash
            assert run["file_skipped"] is False
            assert (run["rows_read"], run["rows_written"]) == (2, 2)
            print("PASS 1: Lần đầu xử lý dữ liệu và lưu đúng file hash")

            # Cùng bytes, kể cả đổi tên file, phải bỏ qua trước pandas.
            (directory / "renamed.csv").write_bytes(
                (directory / "batch.csv").read_bytes()
            )
            with patch.object(
                loader.pd,
                "read_csv",
                side_effect=AssertionError("File đã xử lý vẫn bị phân tích CSV"),
            ):
                for csv_name in ("batch.csv", "renamed.csv"):
                    result = loader.run_orders_ingestion(csv_name, table_name)
                    run = get_run(result["run_id"])
                    assert run["status"] == "SUCCESS"
                    assert run["source_file_hash"] == initial_hash
                    assert run["source_file"] == csv_name
                    assert run["file_skipped"] is True
                    assert (
                        run["rows_read"],
                        run["rows_written"],
                        run["rows_unchanged"],
                    ) == (0, 0, 0)
                    assert snapshot(test_table) == baseline
            print("PASS 2: Cùng nội dung được bỏ qua, kể cả khi đổi tên file")

            # Cùng tên nhưng khác nội dung phải được xử lý.
            changed_hash = write_csv(directory, "batch.csv", [
                make_order("test_01", "delivered"),
                make_order("test_03", "shipped"),
            ])
            result = loader.run_orders_ingestion("batch.csv", table_name)
            run = get_run(result["run_id"])
            after_change = snapshot(test_table)
            assert changed_hash != initial_hash
            assert run["source_file_hash"] == changed_hash
            assert run["file_skipped"] is False
            assert (run["rows_read"], run["rows_written"]) == (2, 2)
            assert len(after_change) == 3
            assert after_change["test_01"]["order_status"] == "delivered"
            assert after_change["test_02"] == baseline["test_02"]
            print("PASS 3: File đổi nội dung được nạp; đơn ngoài batch được giữ")

            result = loader.run_orders_ingestion(
                "batch.csv", table_name, force=True
            )
            run = get_run(result["run_id"])
            assert run["source_file_hash"] == changed_hash
            assert run["file_skipped"] is False
            assert (
                run["rows_read"], run["rows_written"], run["rows_unchanged"]
            ) == (2, 0, 2)
            assert snapshot(test_table) == after_change
            print("PASS 4: force xử lý lại file, row hash vẫn giữ dòng không đổi")

            retry_hash = write_csv(directory, "retry.csv", [
                make_order("test_01", "processing"),
                make_order("test_04", "__force_failure__"),
            ])
            try:
                loader.run_orders_ingestion(
                    "retry.csv", table_name, batch_size=1
                )
            except IntegrityError:
                pass
            else:
                raise AssertionError("CHECK constraint phải gây lỗi")

            assert snapshot(test_table) == after_change
            with loader.engine.connect() as connection:
                run_log = loader.get_pipeline_run_log_table(connection)
                statement = select(run_log).where(
                    run_log.c.target_table == target_table,
                    run_log.c.source_file == "retry.csv",
                )
                failed = dict(connection.execute(statement).mappings().one())
            assert failed["status"] == "FAILED"
            assert failed["rows_written"] == 0
            assert failed["source_file_hash"] == retry_hash
            assert failed["file_skipped"] is False

            # Sửa nguyên nhân lỗi chỉ trên bảng test rồi thử lại cùng bytes.
            with loader.engine.begin() as connection:
                connection.execute(DropConstraint(status_check))

            result = loader.run_orders_ingestion("retry.csv", table_name)
            run = get_run(result["run_id"])
            assert run["status"] == "SUCCESS"
            assert run["source_file_hash"] == retry_hash
            assert run["file_skipped"] is False
            assert (run["rows_read"], run["rows_written"]) == (2, 2)
            assert len(snapshot(test_table)) == 4
            print("PASS 5: FAILED rollback dữ liệu và cho phép thử lại cùng file")

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

    print("Hoàn thành: 5/5 kiểm tra file tracking thành công")


if __name__ == "__main__":
    main()
