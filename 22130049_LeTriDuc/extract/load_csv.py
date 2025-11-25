import pandas as pd
import pymysql
import glob
import os
import re
from datetime import datetime

# Cấu hình kết nối database staging
STAGING_DB_CONFIG = {
    'host': '146.190.93.160',
    'user': 'root',
    'password': '123456',
    'port': 3308,
    'database': 'staging',
    'charset': 'utf8mb4'
}

# Cấu hình kết nối database control
CONTROL_DB_CONFIG = {
    'host': '146.190.93.160',
    'user': 'root',
    'password': '123456',
    'port': 3308,
    'database': 'control',
    'charset': 'utf8mb4'
}

# Tên table trong database
BDS_TABLE_NAME = 'bds_raw'
CHOTOT_TABLE_NAME = 'chotot_raw'


def log_process(cursor, source_id, process_code, process_name, status, started_at=None):
    """Ghi log process vào bảng process_log với status cuối cùng"""
    if started_at is None:
        started_at = datetime.now()

    insert_sql = """
    INSERT INTO process_log (source_id, process_code, process_name, status, started_at)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (source_id, process_code, process_name, status, started_at))
    print(f"Đã insert log process: {process_code} - {status}")
    return started_at


def get_today_csv(pattern):
    """Tìm file CSV có ngày hiện tại theo định dạng pattern_ddmmyyyy.csv"""
    today = datetime.now().strftime('%d%m%Y')
    csv_file = f'../data/{pattern}_{today}.csv'
    if os.path.exists(csv_file):
        return csv_file
    return None


def create_table_if_not_exists(cursor, table_name, num_fields):
    """Tạo table nếu chưa tồn tại. Nếu đã tồn tại thì không drop mà giữ nguyên schema."""
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    exists = cursor.fetchone()

    if exists:
        print(f"Table {table_name} đã tồn tại — giữ nguyên schema, không drop.")

        # Lấy danh sách cột hiện tại
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        current_columns = [col[0] for col in cursor.fetchall() if col[0].startswith("field")]

        if len(current_columns) != num_fields:
            raise Exception(
                f"Schema mismatch: CSV có {num_fields} cột nhưng bảng có {len(current_columns)} cột. "
                f"Bạn cần ALTER TABLE thủ công hoặc xóa bảng một lần."
            )

        return True  # Bảng đã tồn tại

    # Nếu bảng chưa tồn tại -> tạo mới
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
    return False



def load_csv_to_db(csv_file, table_name):
    """Đọc CSV và load vào database"""
    # Đọc CSV, tất cả cột là string (text)
    df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, na_values='')
    num_fields = len(df.columns)

    # Kết nối DB
    connection = pymysql.connect(**STAGING_DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            # Tạo/DROP table để khớp num_fields
            create_table_if_not_exists(cursor, table_name, num_fields)
            # Xóa dữ liệu cũ
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            print(f"Đã TRUNCATE TABLE {table_name}")

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
            return True  # Thành công

    except Exception as e:
        print(f"Lỗi khi load {table_name}: {e}")
        if 'connection' in locals():
            connection.rollback()
        return False  # Thất bại
    finally:
        if 'connection' in locals():
            connection.close()


if __name__ == "__main__":
    # Kết nối DB control để ghi log
    control_connection = None
    try:
        control_connection = pymysql.connect(**CONTROL_DB_CONFIG)
        control_cursor = control_connection.cursor()

        # Log và Load BDS (wrap riêng để lỗi không ảnh hưởng Chotot)
        bds_csv = get_today_csv('bds')
        if bds_csv:
            print(f"Đang load BDS từ file hôm nay: {bds_csv}")
            try:
                success = load_csv_to_db(bds_csv, BDS_TABLE_NAME)
                status = 'SUCCESS' if success else 'FAILED'

                # Log sau khi load xong
                bds_started_at = log_process(control_cursor,1, 'P1', 'load bds staging', status)
                control_connection.commit()
                print(f"Log BDS: {status}")

            except Exception as e:
                print(f"Lỗi BDS process: {e}")
                status = 'FAILED'
                if control_connection:
                    try:
                        bds_started_at = log_process(control_cursor,1, 'P1', 'load bds staging', status)
                        control_connection.commit()
                    except Exception as log_e:
                        print(f"Lỗi log BDS failed: {log_e}")
        else:
            print("Không tìm thấy file BDS hôm nay, bỏ qua.")

        # Log và Load ChoTot (wrap riêng)
        chotot_csv = get_today_csv('chotot')
        if chotot_csv:
            print(f"Đang load ChoTot từ file hôm nay: {chotot_csv}")
            try:
                success = load_csv_to_db(chotot_csv, CHOTOT_TABLE_NAME)
                status = 'SUCCESS' if success else 'FAILED'

                # Log sau khi load xong
                chotot_started_at = log_process(control_cursor,2, 'P2', 'load chotot staging', status)
                control_connection.commit()
                print(f"Log Chotot: {status}")

            except Exception as e:
                print(f"Lỗi Chotot process: {e}")
                status = 'FAILED'
                if control_connection:
                    try:
                        chotot_started_at = log_process(control_cursor, 2,'P2', 'load chotot staging', status)
                        control_connection.commit()
                    except Exception as log_e:
                        print(f"Lỗi log Chotot failed: {log_e}")
        else:
            print("Không tìm thấy file ChoTot hôm nay, bỏ qua.")

        print("Hoàn tất load dữ liệu vào database!")
    except Exception as e:
        print(f"Lỗi tổng (kết nối control): {e}")
    finally:
        if control_connection:
            control_connection.close()