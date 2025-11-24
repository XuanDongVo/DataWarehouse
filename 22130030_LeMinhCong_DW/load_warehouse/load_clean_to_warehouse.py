# load_clean_to_warehouse.py
import pymysql
import pandas as pd
from datetime import datetime, date
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# =====================================================================================
# Step 1 — LOAD CONFIG
#   (Flowchart: "1. Load config")
#   Cấu hình tĩnh cho ETL: DBs, target table, sources, email alert
# =====================================================================================

# CONFIG DB
STAGING_DB = {
    'host': '146.190.93.160',
    'user': 'root',
    'password': '123456',
    'port': 3308,
    'database': 'staging',
    'charset': 'utf8mb4'
}

WAREHOUSE_DB = {
    'host': '146.190.93.160',
    'user': 'root',
    'password': '123456',
    'port': 3308,
    'database': 'warehouse',
    'charset': 'utf8mb4'
}

CONTROL_DB = {
    'host': '146.190.93.160',
    'user': 'root',
    'password': '123456',
    'port': 3308,
    'database': 'control',
    'charset': 'utf8mb4'
}

# TARGET FACT TABLE
TARGET_TABLE = 'bds_common'

# SOURCES cần load (mỗi source = 1 process_code)
SOURCES = {
    'bds': {
        'process_code': 'p5',
        'process_name': 'load to bds DW',
        'staging_table': 'bds_clean',
        'source_id': 1
    },
    'chotot': {
        'process_code': 'p6',
        'process_name': 'load to chotot DW',
        'staging_table': 'chotot_clean',
        'source_id': 2
    }
}

# CONFIG EMAIL ALERT
EMAIL_USER = "22130030@st.hcmuaf.edu.vn"     # email gửi đi
EMAIL_PASS = "njam zyip knfo oskq"           # app password 16 ký tự
EMAIL_TO   = "22130030@st.hcmuaf.edu.vn"     # email nhận cảnh báo


# =====================================================================================
# Notify error & send email
#   (Flowchart: "Notify error and send email"
#               + "Write FAILED and send email")
# =====================================================================================
def send_error_mail(process_code, process_name, error_text):
    """Gửi email khi ETL fail (fail global hoặc fail theo từng source)."""
    try:
        subject = f"❌ ETL FAILED [{process_code}] {process_name}"
        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = EMAIL_TO
        msg["Subject"] = subject

        body = f"""ETL process FAILED

Process: {process_code} - {process_name}
Time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Traceback:
{error_text}
"""
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())

        print(f" → ĐÃ GỬI EMAIL CẢNH BÁO TỚI {EMAIL_TO}")
    except Exception as e:
        print(" → LỖI KHI GỬI EMAIL CẢNH BÁO:", e)


# =====================================================================================
# Step 1 (nhánh Yes/No) — VALIDATE CONFIG
#   (Flowchart: nhánh quyết định sau "1. Load config")
# =====================================================================================
def validate_config():
    """Kiểm tra cấu hình ETL trước khi chạy."""
    required_db_keys = ['host', 'user', 'password', 'port', 'database', 'charset']

    # Check đủ key cho 3 DB
    for name, db_cfg in [('STAGING_DB', STAGING_DB),
                         ('WAREHOUSE_DB', WAREHOUSE_DB),
                         ('CONTROL_DB', CONTROL_DB)]:
        missing = [k for k in required_db_keys if k not in db_cfg]
        if missing:
            raise Exception(f"Config lỗi: {name} thiếu key {missing}")

    # Check SOURCES không rỗng
    if not SOURCES or len(SOURCES) == 0:
        raise Exception("Config lỗi: SOURCES rỗng, không có nguồn để load.")

    # Check mỗi source đủ field
    required_source_keys = ['process_code', 'process_name', 'staging_table', 'source_id']
    for src_name, cfg in SOURCES.items():
        missing = [k for k in required_source_keys if k not in cfg]
        if missing:
            raise Exception(f"Config lỗi: SOURCES['{src_name}'] thiếu key {missing}")

    # Check email config
    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
        raise Exception("Config lỗi: EMAIL_USER / EMAIL_PASS / EMAIL_TO chưa khai báo.")

    print(" → Load config OK ✅ (DB + SOURCES + EMAIL)")


# =====================================================================================
# Step 3 — CHECK TARGET SCHEMA (WAREHOUSE)
#   (Flowchart: "3 Check target schema warehouse.bds_common")
# =====================================================================================
def check_target_schema():
    """Kiểm tra schema bảng warehouse.bds_common đã sẵn sàng để load."""
    conn = pymysql.connect(**WAREHOUSE_DB)
    cur = conn.cursor()

    # Bảng đích có tồn tại?
    cur.execute("SHOW TABLES LIKE 'bds_common'")
    if not cur.fetchone():
        conn.close()
        raise Exception("Table warehouse.bds_common CHƯA được tạo!")

    # Bảng có đủ các cột quan trọng?
    cur.execute("DESCRIBE bds_common")
    columns = [row[0] for row in cur.fetchall()]

    required_columns = [
        'date_key', 'source_id', 'title', 'address', 'province',
        'area_m2', 'price_billion', 'price_per_m2_million',
        'bedroom', 'bathroom', 'floors',
        'house_direction', 'balcony_direction',
        'legal_doc', 'furniture', 'link', 'created_at'
    ]

    missing = sorted(list(set(required_columns) - set(columns)))
    conn.close()

    if missing:
        raise Exception(f"Table warehouse.bds_common THIẾU các cột: {missing}")

    print(" → Schema warehouse.bds_common OK ✅")


# =====================================================================================
# Process log helpers
#   (Flowchart:
#     Step 5  — Write process_log RUNNING
#     Step 13 — Write process_log SUCCESS
#     FAILED  — Write FAILED)
# =====================================================================================
def log_start(cur, code, name, source_id):
    """Step 5 — Ghi log RUNNING khi bắt đầu 1 source."""
    started_at = datetime.now()
    cur.execute(
        """
        INSERT INTO process_log (process_code, process_name, source_id, status, started_at)
        VALUES (%s, %s, %s, 'RUNNING', %s)
        """,
        (code, name, source_id, started_at)
    )
    return cur.lastrowid


def log_success(cur, pid, rows):
    """Step 13 — Update log SUCCESS khi load source OK."""
    finished_at = datetime.now()
    cur.execute(
        """
        UPDATE process_log
        SET status='SUCCESS', update_at=%s
        WHERE process_id=%s
        """,
        (finished_at, pid)
    )


def log_failed(cur, pid):
    """FAILED — Update log FAILED khi load source lỗi."""
    finished_at = datetime.now()
    cur.execute(
        """
        UPDATE process_log
        SET status='FAILED', update_at=%s
        WHERE process_id=%s
        """,
        (finished_at, pid)
    )


# =====================================================================================
# Step 6 — CHECK STAGING TABLE EXISTS & HAS ROWS
#   (Flowchart: "6 Check staging table exists and has rows")
# =====================================================================================
def check_staging_table_has_data(table_name):
    """
    Step 6:
      - Kiểm tra table staging.<table_name> có tồn tại không
      - Và có ít nhất 1 dòng dữ liệu không
    """
    conn = pymysql.connect(**STAGING_DB)
    cur = conn.cursor()

    # check tồn tại
    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (STAGING_DB['database'], table_name))
    exists = cur.fetchone()[0] > 0

    if not exists:
        conn.close()
        raise Exception(f"Table {STAGING_DB['database']}.{table_name} KHÔNG tồn tại!")

    # check row_count
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cur.fetchone()[0]
    conn.close()

    if row_count == 0:
        raise Exception(f"Table {STAGING_DB['database']}.{table_name} KHÔNG có dữ liệu (row_count = 0)!")

    print(f" → {STAGING_DB['database']}.{table_name} OK, row_count = {row_count}")
    return row_count


# =====================================================================================
# Step 8 — VALIDATE DATA QUALITY
#   (Flowchart: "8 Validate data quality")
# =====================================================================================
def validate_staging_df(df, required_cols, numeric_positive_cols, key_cols=None):
    """
    Step 8:
      - required_cols: cột bắt buộc không NULL
      - numeric_positive_cols: cột số phải > 0
      - key_cols: check duplicate (nếu có)
    """
    problems = {}

    # missing cols
    missing_cols = [c for c in required_cols if c not in df.columns]
    problems['missing_cols'] = missing_cols

    # null count
    if missing_cols:
        null_count = None
    else:
        null_mask = df[required_cols].isna().any(axis=1)
        null_count = int(null_mask.sum())
    problems['null_count'] = null_count

    # dup count
    if key_cols:
        dup_count = int(df.duplicated(subset=key_cols).sum())
    else:
        dup_count = 0
    problems['dup_count'] = dup_count

    # invalid numeric <=0
    invalid_count = 0
    for col in numeric_positive_cols:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors='coerce')
            invalid_count += int((vals <= 0).sum())
    problems['invalid_count'] = invalid_count

    problems['is_valid'] = (
        (not missing_cols) and
        (null_count == 0) and
        (dup_count == 0) and
        (invalid_count == 0)
    )

    return problems


# =====================================================================================
# Step 10 — ENSURE date_key IN date_dim
#   (Flowchart: "10 Ensure date_key in date_dim")
# =====================================================================================
def ensure_date_in_dim(conn_wh, date_key_int):
    """
    Step 10:
      - Đảm bảo date_key tồn tại trong warehouse.date_dim
      - Nếu chưa có thì tự insert 1 dòng
    """
    cur = conn_wh.cursor()
    cur.execute("SELECT 1 FROM date_dim WHERE dateKey=%s LIMIT 1", (date_key_int,))
    if cur.fetchone():
        return

    s = str(date_key_int)
    d = datetime.strptime(s, "%Y%m%d").date()

    insert_sql = """
        INSERT INTO date_dim
        (dateKey, fullDate, dayOfDate, monthOfDate, monthName,
         quaterOfDate, yearOfDate, weekOfDate)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cur.execute(insert_sql, (
        int(s),
        d.strftime("%Y-%m-%d"),
        d.day,
        d.month,
        d.strftime("%B"),
        (d.month - 1)//3 + 1,
        d.year,
        int(d.strftime("%U"))
    ))
    conn_wh.commit()
    print(f" → Auto insert date_dim for dateKey={date_key_int}")


# =====================================================================================
# Safe helpers for Step 11 (Map/Transform)
# =====================================================================================
def safe_str(df, col, default=None):
    """Làm sạch text column."""
    if col not in df.columns:
        return pd.Series([default] * len(df), dtype="object")

    s = df[col].astype(str)
    s = s.str.replace(r"[\r\n\t]+", " ", regex=True)
    s = s.replace(["nan", "None"], None)
    s = s.str.strip()
    s = s.where(s.notna(), default)
    return s


def safe_num(df, col, default=0):
    """Ép kiểu số, thiếu cột thì default."""
    if col not in df.columns:
        return pd.Series([default] * len(df), dtype="float64")
    return pd.to_numeric(df[col], errors='coerce').fillna(default)


# =====================================================================================
# LOAD 1 SOURCE PIPELINE (Step 5 → Step 13)
# =====================================================================================
def load_source(cfg, conn_control):
    """
    cfg: config của 1 source trong SOURCES
    conn_control: connection CONTROL_DB để ghi process_log
    """
    cur_control = conn_control.cursor()

    # Step 5 — Write process_log RUNNING
    pid = log_start(cur_control, cfg['process_code'],
                    cfg['process_name'], cfg['source_id'])

    try:
        print(f"\n[{cfg['process_code']}] {cfg['process_name']} → Bắt đầu")

        # Step 6 — Check staging table exists & has rows
        check_staging_table_has_data(cfg['staging_table'])

        # Step 7 — Read staging to dataframe
        conn_staging = pymysql.connect(**STAGING_DB)
        df = pd.read_sql(f"SELECT * FROM {cfg['staging_table']}", conn_staging)
        conn_staging.close()
        print(f" → Đọc thành công {len(df):,} dòng")

        # Step 8 — Validate data quality
        candidate_required = ['title', 'tieu_de', 'area_m2']
        required_cols = [c for c in candidate_required if c in df.columns]
        numeric_positive_cols = [c for c in ['area_m2'] if c in df.columns]
        key_cols = None

        dq = validate_staging_df(df, required_cols, numeric_positive_cols, key_cols)
        print(" → Data quality check:", dq)

        if (
            (dq['null_count'] not in (0, None)) or
            dq['dup_count'] > 0 or
            dq['invalid_count'] > 0
        ):
            raise Exception(f"Data quality FAILED for {cfg['staging_table']}: {dq}")

        # Step 9 — Set date_key = ETL today
        etl_date_key = int(datetime.now().strftime("%Y%m%d"))
        date_key = pd.Series([etl_date_key] * len(df), dtype="int64")

        # Step 10 — Ensure date_key in date_dim
        conn_wh_tmp = pymysql.connect(**WAREHOUSE_DB)
        ensure_date_in_dim(conn_wh_tmp, etl_date_key)
        ensure_date_in_dim(conn_wh_tmp, 19000101)   # fallback date_key
        conn_wh_tmp.close()

        # created_at ưu tiên ngày đăng (nếu có)
        date_cols = [
            'post_date', 'date', 'ngay_dang',
            'date_posted', 'created_date',
            'posted_date', 'ngay_dang_tin'
        ]
        date_series = None
        for c in date_cols:
            if c in df.columns:
                date_series = pd.to_datetime(df[c], errors='coerce')
                break
        if date_series is None:
            date_series = pd.Series([pd.NaT] * len(df))

        created_at_series = date_series.fillna(datetime.now())

        # Step 11 — Map and transform columns
        # ----- Price theo từng nguồn -----
        if cfg['source_id'] == 1:
            # BDS: staging thường đã có price_billion
            if 'price_billion' in df.columns:
                price_billion = safe_num(df, 'price_billion', 0)
            else:
                # fallback từ price/gia (giả sử triệu)
                if 'price' in df.columns:
                    price_billion = safe_num(df, 'price', 0) / 1_000.0
                elif 'gia' in df.columns:
                    price_billion = safe_num(df, 'gia', 0) / 1_000.0
                else:
                    price_billion = pd.Series([0] * len(df), dtype="float64")

        elif cfg['source_id'] == 2:
            # Chợ Tốt: price thường là VND
            if 'price_billion' in df.columns:
                price_billion = safe_num(df, 'price_billion', 0)
            elif 'price' in df.columns:
                price_vnd = safe_num(df, 'price', 0)
                price_billion = price_vnd / 1_000_000_000.0
            else:
                price_billion = pd.Series([0] * len(df), dtype="float64")

        else:
            # phòng khi thêm nguồn khác sau này
            if 'price_billion' in df.columns:
                price_billion = safe_num(df, 'price_billion', 0)
            elif 'price' in df.columns:
                price_billion = safe_num(df, 'price', 0)
            elif 'gia' in df.columns:
                price_billion = safe_num(df, 'gia', 0)
            else:
                price_billion = pd.Series([0] * len(df), dtype="float64")

        # ----- Text columns chuẩn hoá theo schema fact -----
        if 'title' in df.columns:
            title = safe_str(df, 'title', 'Không có tiêu đề')
        elif 'tieu_de' in df.columns:
            title = safe_str(df, 'tieu_de', 'Không có tiêu đề')
        else:
            title = safe_str(df, '_no_col_', 'Không có tiêu đề')

        address = safe_str(df, 'address', None) if 'address' in df.columns else safe_str(df, 'dia_chi', None)
        province = safe_str(df, 'province', None) if 'province' in df.columns else safe_str(df, 'tinh_thanh', None)
        house_direction = safe_str(df, 'house_direction', None) if 'house_direction' in df.columns else safe_str(df, 'huong_nha', None)
        balcony_direction = safe_str(df, 'balcony_direction', None) if 'balcony_direction' in df.columns else safe_str(df, 'huong_ban_cong', None)
        legal_doc = safe_str(df, 'legal_doc', None) if 'legal_doc' in df.columns else safe_str(df, 'giay_to_phap_ly', None)
        furniture = safe_str(df, 'furniture', None) if 'furniture' in df.columns else safe_str(df, 'noi_that', None)
        link = safe_str(df, 'link', None) if 'link' in df.columns else safe_str(df, 'url', None)

        # ----- Num columns chuẩn hoá -----
        area_m2 = safe_num(df, 'area_m2', 0)
        price_per_m2_million = (price_billion / (area_m2 + 0.0001)).round(3)

        bedroom = safe_num(df, 'bedroom', -1).astype(int)
        bathroom = safe_num(df, 'bathroom', -1).astype(int)
        floors = safe_num(df, 'floors', -1).astype(int)

        # Build df_insert final
        df_insert = pd.DataFrame({
            'date_key': date_key,
            'source_id': cfg['source_id'],
            'title': title,
            'address': address,
            'province': province,
            'area_m2': area_m2,
            'price_billion': price_billion,
            'price_per_m2_million': price_per_m2_million,
            'bedroom': bedroom,
            'bathroom': bathroom,
            'floors': floors,
            'house_direction': house_direction,
            'balcony_direction': balcony_direction,
            'legal_doc': legal_doc,
            'furniture': furniture,
            'link': link,
            'created_at': created_at_series
        })

        # Step 12 — Insert rows to warehouse.bds_common
        conn_wh = pymysql.connect(**WAREHOUSE_DB)
        cur_wh = conn_wh.cursor()

        cols = ", ".join([f"`{c}`" for c in df_insert.columns])
        placeholders = ", ".join(["%s"] * len(df_insert.columns))
        sql = f"INSERT INTO {TARGET_TABLE} ({cols}) VALUES ({placeholders})"
        data = [tuple(row) for row in df_insert.values]

        print(f" → Đang insert {len(data):,} dòng...")
        for i in range(0, len(data), 5000):
            cur_wh.executemany(sql, data[i:i + 5000])
            conn_wh.commit()
        conn_wh.close()

        # Step 13 — Write process_log SUCCESS
        log_success(cur_control, pid, len(data))
        conn_control.commit()
        print(f" → HOÀN TẤT THÀNH CÔNG: {len(data):,} dòng vào warehouse!\n")

    except Exception:
        # Nếu bất kỳ bước nào fail trong source:
        #   - Write FAILED log
        #   - Send email
        #   - Tiếp tục source tiếp theo
        err_txt = traceback.format_exc()
        print(f" → LỖI [{cfg['process_code']}]: {err_txt}")
        log_failed(cur_control, pid)
        conn_control.commit()
        send_error_mail(cfg['process_code'], cfg['process_name'], err_txt)


# =====================================================================================
# MAIN / GLOBAL WORKFLOW
#   Step 1 → Step 4 trong flowchart
# =====================================================================================
if __name__ == "__main__":
    try:
        print("\n" + "=" * 100)
        print("CHẠY ETL LOAD STAGING → WAREHOUSE")
        print("=" * 100)

        # Step 1 — Load config (kèm validate nhánh Yes/No)
        validate_config()

        # Step 2 — Connect to CONTROL_DB
        conn_control = pymysql.connect(**CONTROL_DB)

        try:
            # Step 3 — Check target schema warehouse.bds_common
            check_target_schema()

            # Step 4 — For each source in SOURCES
            for cfg in SOURCES.values():
                load_source(cfg, conn_control)

            print("=" * 100)
            print("DONE – DỮ LIỆU ĐÃ VÀO FACT TABLE bds_common!")
            print("=" * 100)

        finally:
            conn_control.close()

    except Exception:
        # Lỗi global (fail ở Step 1/2/3 trước khi chạy sources)
        err_txt = traceback.format_exc()
        print(" → LỖI GLOBAL (load config / init / connect control):\n", err_txt)

        # Notify error and send email (global)
        send_error_mail("GLOBAL", "Load Config / Init ETL", err_txt)
