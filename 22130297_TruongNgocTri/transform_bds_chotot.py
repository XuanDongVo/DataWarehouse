import re
from datetime import datetime
import traceback

import pandas as pd
from sqlalchemy import create_engine, text
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# CẤU HÌNH KẾT NỐI DB
STAGING_DB_URL = "mysql+pymysql://root:123456@146.190.93.160:3308/staging"
CONTROL_DB_URL = "mysql+pymysql://root:123456@146.190.93.160:3308/control"
staging_engine = create_engine(STAGING_DB_URL)
control_engine = create_engine(CONTROL_DB_URL)

# CẤU HÌNH PROCESS
PROCESS_BDS = "TRANSFORM_BDS"
PROCESS_CHOTOT = "TRANSFORM_CHOTOT"
SOURCE_BDS_NAME = "BATDONGSAN"
SOURCE_CHOTOT_NAME = "CHOTOT"

# CẤU HÌNH EMAIL
SMTP_SERVER   = "smtp-relay.brevo.com"
SMTP_PORT     = 587
SMTP_USERNAME = "9c0274001@smtp-brevo.com"
SMTP_PASSWORD = "KvckRSgMXqdj7GwH"
EMAIL_FROM    = "9c0274001@smtp-brevo.com"
EMAIL_TO      = ["22130297@st.hcmuaf.edu.vn"]

# Đường dẫn CSV đầu ra
TODAY = datetime.now().strftime("%d%m%Y")
BASE_CSV_DIR = "/D/DW/staging/data/" 
BDS_CSV_PATH = f"{BASE_CSV_DIR}bds_clean_{TODAY}.csv"
CHOTOT_CSV_PATH = f"{BASE_CSV_DIR}chotot_clean_{TODAY}.csv"

# --- HELPER: gửi email (fix lỗi join) ---
def send_error_email(process_name: str, error_message: str):
    subject = f"[LỖI ETL] {process_name} thất bại - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = f"""
    <h2>ETL Transform thất bại!</h2>
    <p><b>Process:</b> {process_name}</p>
    <p><b>Thời gian:</b> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
    <p><b>Lỗi chi tiết:</b></p>
    <pre>{error_message}</pre>
    <p>Vui lòng kiểm tra server ngay!</p>
    """

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(EMAIL_TO) if isinstance(EMAIL_TO, (list, tuple)) else EMAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        recipients = EMAIL_TO if isinstance(EMAIL_TO, (list, tuple)) else [EMAIL_TO]
        server.sendmail(EMAIL_FROM, recipients, msg.as_string())
        server.quit()
        print("Đã gửi email báo lỗi thành công!")
    except Exception as e:
        print(f"Gửi email lỗi thất bại: {e}")

# --- GET SOURCE ID (và kiểm tra) ---
def get_source_id(source_name: str):
    try:
        sql = text("SELECT source_id FROM config_source WHERE source_name = :name LIMIT 1")
        with control_engine.begin() as conn:
            row = conn.execute(sql, {"name": source_name}).first()
        return row[0] if row else None
    except Exception as e:
        raise Exception(f"Không tìm thấy source_id cho {source_name}: {e}")

# --- LOG PROCESS START ---
def log_process_start(process_name: str, source_id: int = None):
    start_time = datetime.now()
    process_code_map = {
        PROCESS_BDS: "P3",
        PROCESS_CHOTOT: "P4"
    }
    process_code = process_code_map.get(process_name, "unknown")
    try:
        sql = text("""
            INSERT INTO process_log (process_code, process_name, started_at, status, source_id)
            VALUES (:process_code, :process_name, :started_at, 'PROCESS', :source_id)
        """)
        with control_engine.begin() as conn:
            conn.execute(sql, {
                "process_code": process_code,
                "process_name": process_name,
                "started_at": start_time,
                "source_id": source_id
            })
            # Lấy id vừa insert
            res = conn.execute(text("SELECT LAST_INSERT_ID()"))
            process_id = res.scalar()
        if not process_id:
            raise Exception("Không lấy được process_id sau khi insert process_log")
        return process_id
    except Exception as e:
        raise Exception(f"Lỗi khi insert process_log start: {e}")

# --- LOG PROCESS END ---
def log_process_end(process_id: int = None, status: str = "FAILED", message: str = None, process_code: str = None, process_name: str = None, source_id: int = None):
    end_time = datetime.now()
    sql = text("""
        UPDATE process_log
        SET update_at = :update_at,
            status = :status,
            process_name = CONCAT(process_name, ' - ', :message)
        WHERE process_id = :process_id
    """)
    with control_engine.begin() as conn:
        conn.execute(sql, {
            "update_at": end_time,
            "status": status,
            "message": message or '',
            "process_id": process_id
        })

# --- FILE LOG ---
def log_file(table_name: str, source_id: int, row_count: int, status="SUCCESS"):
    now = datetime.now()
    try:
        sql = text("""
            INSERT INTO file_log (file_path, time, count, size, status, execute_time, source_id)
            VALUES (:file_path, :time, :count, :size, :status, :execute_time, :source_id)
        """)
        with control_engine.begin() as conn:
            conn.execute(sql, {
                "file_path": f"{table_name}.csv",
                "time": now,
                "count": row_count,
                "size": None,
                "status": status,
                "execute_time": 0,
                "source_id": source_id
            })
        return True
    except Exception as e:
        return False

# --- Hàm xử lý thất bại chung (ghi log FAILED + gửi email) ---
def fail_and_report(process_id, process_name, source_id, error_message, process_code=None):
    # Cập nhật process_log (FAILED)
    log_process_end(process_id, "FAILED", error_message, process_code=process_code, process_name=process_name, source_id=source_id)
    # Gửi email báo lỗi
    send_error_email(process_name, error_message)

# Hàm làm sạch chuỗi số (lấy số từ "80 m²", "5 tỷ", "3,5 triệu" …)
def extract_number(value: str):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).lower().strip()
    match = re.findall(r"[0-9]+[.,]?[0-9]*", text)
    if not match:
        return None
    num = match[0].replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None

# Hàm chuẩn hóa diện tích (m²)
def normalize_area(area_str: str):
    return extract_number(area_str)

# Hàm chuẩn hóa phòng ngủ / phòng vệ sinh (lấy số nguyên)
def normalize_int(value: str):
    num = extract_number(value)
    return int(num) if num is not None else None

# Hàm chuẩn hóa giá cho BĐS từ chuỗi tiếng Việt
def normalize_price_vnd(price_str: str):
    if not price_str:
        return None
    text = str(price_str).lower().strip()
    number = extract_number(text)
    if number is None:
        return None
    if "tỷ" in text or "ty" in text:
        return number * 1_000_000_000
    elif "triệu" in text or "trieu" in text:
        return number * 1_000_000
    elif "nghìn" in text or "nghin" in text or "k" in text:
        return number * 1_000
    else:
        return number

# --- KIỂM TRA BẢNG RAW CÓ DỮ LIỆU KHÔNG ---
def check_table_has_data(table_name: str):
    try:
        sql = text(f"SELECT COUNT(*) as cnt FROM {table_name}")
        with staging_engine.connect() as conn:
            result = conn.execute(sql).scalar()
        if not result or int(result) == 0:
            raise Exception(f"Bảng {table_name} chưa có dữ liệu!")
        return int(result)
    except Exception as e:
        raise Exception(f"Lỗi khi kiểm tra dữ liệu bảng {table_name}: {e}")


# ---------------- TRANSFORM BDS ----------------
def transform_bds(source_id: int = None, process_id: int = None):
    process_name = PROCESS_BDS
    try:
        # 3.1.3 Kiểm tra bảng raw có dữ liệu không
        check_table_has_data("bds_raw")

        # 3.1.4 Đọc dữ liệu raw
        try:
            df_raw = pd.read_sql("SELECT * FROM bds_raw", staging_engine)
        except Exception as e:
            raise Exception(f"Không đọc được bds_raw")

        # 3.1.5 Kiểm tra thiếu cột và rename
        column_mapping = {
            "field1": "link",
            "field2": "title",
            "field3": "area_raw",
            "field4": "balcony_direction",
            "field5": "house_direction",
            "field6": "price_raw",
            "field7": "front_width",
            "field8": "furniture",
            "field9": "legal_doc",
            "field10": "bedroom_raw",
            "field11": "bathroom_raw",
            "field12": "floor_raw",
            "field13": "entrance_width"
        }
        missing_cols = [c for c in column_mapping if c not in df_raw.columns]
        if missing_cols:
            raise Exception(f"Thiếu cột trong bds_raw: {missing_cols}")

        # Rename và chọn cột cần thiết
        df = df_raw[list(column_mapping.keys())].rename(columns=column_mapping)

        # 3.1.6 Transform dữ liệu
        try:
            df["area_m2"] = df["area_raw"].apply(normalize_area)
            df["price_vnd"] = df["price_raw"].apply(normalize_price_vnd)
            df["bedroom"] = df["bedroom_raw"].apply(normalize_int)
            df["bathroom"] = df["bathroom_raw"].apply(normalize_int)
            df["floors"] = df["floor_raw"].apply(normalize_int)

            df[["area_m2", "price_vnd", "bedroom", "bathroom", "floors"]] = \
                df[["area_m2", "price_vnd", "bedroom", "bathroom", "floors"]].fillna(-1)
            df[["bedroom", "bathroom", "floors"]] = df[["bedroom", "bathroom", "floors"]].astype(int)

            if "created_at" not in df_raw.columns:
                df["created_at"] = datetime.now()
            else:
                df["created_at"] = df_raw["created_at"]

            df["date_key"] = pd.to_datetime(df["created_at"]).dt.strftime('%Y%m%d').astype(int)
            df["transformed_at"] = datetime.now()
        except Exception as e:
            raise Exception(f"Transform thất bại: {e}")

        #Chọn cột final
        final_cols = [
            "link", "title", "area_raw", "balcony_direction", "house_direction",
            "price_raw", "front_width", "furniture", "legal_doc", "bedroom_raw",
            "bathroom_raw", "floor_raw", "entrance_width", "area_m2", "price_vnd",
            "bedroom", "bathroom", "floors", "date_key", "created_at", "transformed_at"
        ]
        df_clean = df[[c for c in final_cols if c in df.columns]]

        # 3.1.7. Load vào staging.bds_clean
        try:
            with staging_engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE bds_clean"))
            df_clean.to_sql("bds_clean", staging_engine, if_exists="append", index=False)
            df_clean.to_csv(BDS_CSV_PATH, index=False, encoding="utf-8-sig")
        except Exception as e:
            raise Exception(f"Load bds_clean thất bại: {e}")

        # 3.1.8. Ghi file_log
        try:
            ok = log_file("bds_clean", source_id, len(df_clean))
            if not ok:
                raise Exception("Ghi file_log thất bại")
        except Exception as e:
            raise Exception(f"Ghi file_log thất bại")

        return len(df_clean)

    except Exception as e:
        # Báo lỗi lên process_log + gửi email
        tb = traceback.format_exc()
        error_message = f"{str(e)}\n\nTraceback:\n{tb}"
        fail_and_report(process_id, process_name, source_id, error_message, process_code="P3")
        raise


# ---------------- TRANSFORM CHOTOT ----------------
def transform_chotot(source_id: int = None, process_id: int = None):
    process_name = PROCESS_CHOTOT
    try:
        # 3.1.10 Kiểm tra bảng raw có dữ liệu không
        check_table_has_data("chotot_raw")
            
        # 3.1.11. Đọc dữ liệu raw
        try:
            df_raw = pd.read_sql("SELECT * FROM chotot_raw", staging_engine)
        except Exception as e:
            raise Exception(f"Không đọc được chotot_raw")

        # 3.1.12. Kiểm tra cột và rename
        column_mapping = {
            "field1": "title",
            "field2": "address",
            "field3": "area_desc",
            "field4": "size_raw",
            "field5": "bedroom_raw",
            "field6": "area_m2",
            "field7": "price_raw",
            "field8": "province",
            "field9": "country",
        }
        missing_cols = [c for c in column_mapping if c not in df_raw.columns]
        if missing_cols:
            raise Exception(f"Thiếu cột trong chotot_raw")

        df = df_raw[list(column_mapping.keys())].rename(columns=column_mapping)

        # 3.1.13 Transform dữ liệu
        try:
            df["bedroom"] = df["bedroom_raw"].apply(normalize_int)
            df["price_vnd"] = df["price_raw"].apply(normalize_price_vnd)
            df["price_val_million"] = df["price_vnd"] / 1_000_000
            df["price_billion"] = df["price_vnd"] / 1_000_000_000

            df[["bedroom", "price_vnd", "price_val_million", "price_billion"]] = \
                df[["bedroom", "price_vnd", "price_val_million", "price_billion"]].fillna(-1)
            df["bedroom"] = df["bedroom"].astype(int)

            if "created_at" not in df_raw.columns:
                df["created_at"] = datetime.now()
            else:
                df["created_at"] = df_raw["created_at"]

            df["date_key"] = pd.to_datetime(df["created_at"]).dt.strftime('%Y%m%d').astype(int)
            df["transformed_at"] = datetime.now()
        except Exception as e:
            raise Exception(f"Transform thất bại: {e}")

        final_cols = [
            "title", "address", "area_desc", "size_raw", "bedroom_raw",
            "area_m2", "price_raw", "province", "country",
            "bedroom", "price_vnd", "price_val_million", "price_billion",
            "date_key", "created_at", "transformed_at"
        ]
        df_clean = df[[c for c in final_cols if c in df.columns]]

        # 3.1.14.Load vào staging và xuất csv
        try:
            with staging_engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE chotot_clean"))
            df_clean.to_sql("chotot_clean", staging_engine, if_exists="append", index=False)
            df_clean.to_csv(CHOTOT_CSV_PATH, index=False, encoding="utf-8-sig")
        except Exception as e:
            raise Exception(f"Load chotot_clean thất bại: {e}")

        # 3.1.15 Ghi file_log
        try:
            ok = log_file("chotot_clean", source_id, len(df_clean))
            if not ok:
                raise Exception("Ghi file_log thất bại")
        except Exception as e:
            raise Exception(f"Ghi file_log thất bại")

        return len(df_clean)

    except Exception as e:
        # Báo lỗi lên process_log + gửi email
        tb = traceback.format_exc()
        error_message = f"{str(e)}\n\nTraceback:\n{tb}"
        fail_and_report(process_id, process_name, source_id, error_message, process_code="P4")
        raise


# HÀM RUNNER tổng quát
def run_transform_for_source(process_name: str, source_name: str, transform_func):
    source_id = None
    process_id = None
    # 3.1.1. Lấy source_id
    try:
        source_id = get_source_id(source_name)
        if not source_id:
            raise Exception(f"Không tìm thấy source_id")
    except Exception as e:
        error_message = f"Không tìm thấy source_id"
        log_process_end(None, "FAILED", error_message, process_code="unknown", process_name=process_name, source_id=None)
        send_error_email(process_name, error_message)
        print(error_message)
        return

    # 3.1.2. Log process start 
    try:
        process_id = log_process_start(process_name, source_id)
    except Exception as e:
        tb = traceback.format_exc()
        error_message = f"Lỗi khi insert process_log start: {e}"
        log_process_end(None, "FAILED", error_message, process_code="unknown", process_name=process_name, source_id=source_id)
        send_error_email(process_name, error_message)
        return

    # THỰC HIỆN TRANSFORM 
    row_count = transform_func(source_id=source_id, process_id=process_id)

    # 3.1.9, 3.1.16 Nếu thành công -> log process end SUCCESS và in
    msg = f"OK - {row_count} rows"
    log_process_end(process_id, "SUCCESS", msg)
    print(f"{process_name}: {msg}")

def main():
    # BDS Branch
    run_transform_for_source(PROCESS_BDS, SOURCE_BDS_NAME, transform_bds)
    # Chotot Branch
    run_transform_for_source(PROCESS_CHOTOT, SOURCE_CHOTOT_NAME, transform_chotot)

if __name__ == "__main__":
    main()