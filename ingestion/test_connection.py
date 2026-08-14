import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# Đọc các biến cấu hình trong file .env
load_dotenv(override=True)

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# Tạo địa chỉ kết nối PostgreSQL
database_url = (
    f"postgresql+psycopg2://{db_user}:{db_password}"
    f"@{db_host}:{db_port}/{db_name}"
)

# Tạo đối tượng quản lý kết nối database
engine = create_engine(database_url)

# Mở kết nối, chạy SQL kiểm tra rồi tự đóng kết nối
with engine.connect() as connection:
    result = connection.execute(
        text("SELECT current_database(), current_user;")
    )
    database, user = result.fetchone()

    print("Kết nối PostgreSQL thành công!")
    print(f"Database: {database}")
    print(f"User: {user}")