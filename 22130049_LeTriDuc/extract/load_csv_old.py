import pandas as pd
import pymysql
import glob
import os
import re
from datetime import datetime

# Cấu hình kết nối database
DB_CONFIG = {
    'host': '146.190.93.160',
    'user': 'root',
    'password': '123456',
    'port': 3308,
    'database': 'staging',
    'charset': 'utf8mb4'
}

# Tên table trong database
BDS_TABLE_NAME = 'bds_raw'
CHOTOT_TABLE_NAME = 'chotot_raw'


def get_today_csv(pattern):
    """Tìm file CSV có ngày hiện tại theo định dạng pattern_ddmmyyyy.csv"""
    today = datetime.now().strftime('%d%m%Y')
    csv_file = f'../data/{pattern}_{today}.csv'
    if os.path.exists(csv_file):
        return csv_file
    return None


def create_table_if_not_exists(cursor, table_name, num_fields):
    """Tạo table nếu chưa tồn tại, với các cột field1, field2, ... là TEXT"""
    # Kiểm tra table tồn tại
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if cursor.fetchone():
        print(f"Table {table_name} đã tồn tại. Sẽ truncate dữ liệu cũ.")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        return True  # Đã tồn tại và truncate
    else:
        # Tạo table mới
        fields_def = ', '.join([f'field{i + 1} TEXT' for i in range(num_fields)])
        create_sql = f"""
        CREATE TABLE {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {fields_def},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        cursor.execute(create_sql)
        print(f"Đã tạo table {table_name} với {num_fields} cột field.")
        return False  # Mới tạo, không cần truncate


def load_csv_to_db(csv_file, table_name):
    """Đọc CSV và load vào database"""
    # Đọc CSV, tất cả cột là string (text)
    df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, na_values='')
    num_fields = len(df.columns)

    # Kết nối DB
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            # Tạo table nếu cần và truncate nếu tồn tại
            create_table_if_not_exists(cursor, table_name, num_fields)

            # Chuẩn bị dữ liệu: Chuyển df thành list of dicts với key 'field1', 'field2', ...
            data_to_insert = []
            for _, row in df.iterrows():
                row_dict = {f'field{i + 1}': str(val) if pd.notna(val) else '' for i, val in enumerate(row)}
                data_to_insert.append(row_dict)

            # Insert từng row (vì cột động, dùng executemany với placeholders)
            placeholders = ', '.join(['%s'] * num_fields)
            insert_sql = f"""
            INSERT INTO {table_name} ({', '.join([f'field{i + 1}' for i in range(num_fields)])})
            VALUES ({placeholders})
            """
            field_names = [f'field{i + 1}' for i in range(num_fields)]
            values = [tuple(row_dict[field] for field in field_names) for row_dict in data_to_insert]

            cursor.executemany(insert_sql, values)
            connection.commit()
            print(f"Đã insert {len(values)} rows vào table {table_name}")

    finally:
        connection.close()


if __name__ == "__main__":
    try:
        # Load BDS
        bds_csv = get_today_csv('bds')
        if bds_csv:
            print(f"Đang load BDS từ file hôm nay: {bds_csv}")
            load_csv_to_db(bds_csv, BDS_TABLE_NAME)
        else:
            print("Không tìm thấy file BDS hôm nay, bỏ qua.")

        # Load ChoTot
        chotot_csv = get_today_csv('chotot')
        if chotot_csv:
            print(f"Đang load ChoTot từ file hôm nay: {chotot_csv}")
            load_csv_to_db(chotot_csv, CHOTOT_TABLE_NAME)
        else:
            print("Không tìm thấy file ChoTot hôm nay, bỏ qua.")

        print("Hoàn tất load dữ liệu vào database!")
    except Exception as e:
        print(f"Lỗi: {e}")