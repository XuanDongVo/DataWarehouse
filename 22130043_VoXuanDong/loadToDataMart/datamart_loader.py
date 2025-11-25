import json
import pymysql
import csv
import os
import glob
from datetime import datetime
import sys
import traceback

# Import email utils từ folder aggregate 
from email_utils import send_error_email

class DataMartLoader:
    """Base class cho data mart loader - kế thừa từ DatabaseLoader pattern"""
    
    def __init__(self):
        self.cfg = None
        self.conn = None
        self.process_id = None
    
    def load_config(self, config_source="/D/DW/control/config_datamart.json"):
         # 1️ LOAD CONFIG
        """Load JSON config từ file"""
        try:
            if not os.path.exists(config_source):
                raise FileNotFoundError(f"Config file not found at: {config_source}")
            with open(config_source, "r", encoding="utf-8") as f:
                self.cfg = json.load(f)
            return self.cfg

        except Exception as e:
            print(f"ERROR: Cannot load config.json — {e}")
            raise e

    def get_connection(self):
         # 2️ CONNECT DATABASE
        """Kết nối database"""
        db_cfg = self.cfg["database"]
        try:
            self.conn = pymysql.connect(
                host=db_cfg["host"],
                user=db_cfg["user"],
                password=db_cfg["password"],
                port=db_cfg["port"],
                database=db_cfg.get("database"),
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Database connected successfully")
            return self.conn
        except Exception as e:
            print("ERROR: Cannot connect to database")
            raise e

    def load_control_record(self, control_table, process_code, source_id):
        """Lấy control record từ database"""
        sql = f"""
            SELECT *
            FROM {control_table}
            WHERE process_code = %s
              AND source_id = %s
              AND DATE(started_at) = CURDATE()
            ORDER BY process_id DESC
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (process_code, source_id))
            return cur.fetchone()
    
    # 4️ CHECK DEPENDENCIES
    def check_dependencies(self, control_table, dependencies, process_name=""):
        """Kiểm tra dependencies"""
        sql = f"""
            SELECT *
            FROM {control_table}
            WHERE process_code = %s
            AND source_id = %s
            AND DATE(started_at) = CURDATE()
            ORDER BY process_id DESC
            LIMIT 1
        """

        for dep in dependencies:
            dep_code = dep["process_code"]
            dep_src = dep["source_id"]

            print(f"Checking dependency: {dep_code} / {dep_src}")

            with self.conn.cursor() as cur:
                cur.execute(sql, (dep_code, dep_src))
                row = cur.fetchone()
             # Không tồn tại record
            if not row:
                error_msg = f"Không tìm thấy process_log hôm nay cho ({dep_code}, {dep_src})"
                print("ERROR:", error_msg)
                send_error_email(self.cfg, f"[ERROR] {process_name} Dependency Missing", error_msg)
                return False
             # Status != SUCCESS
            if row["status"] != "SUCCESS":
                error_msg = f"Process ({dep_code}, {dep_src}) chưa SUCCESS (status={row['status']})"
                print("ERROR:", error_msg)
                send_error_email(self.cfg, f"[ERROR] {process_name} Dependency Failed", error_msg)
                return False

            print(f"Dependency OK: {dep_code}/{dep_src} = SUCCESS")

        return True

    # 5️ CHECK CURRENT PROCESS
    def check_current_process(self, control_table, process_code, source_id):
        """Kiểm tra process hiện tại để tránh duplicate"""
        record_curr = self.load_control_record(control_table, process_code, source_id)
        if record_curr:
            if record_curr["status"] in ("PROCESS", "SUCCESS"):
                print(f"INFO: Process {process_code} đang chạy hoặc đã xong → STOP")
                return "SKIP"
            elif record_curr["status"] == "FAIL":
                print(f"INFO: Process {process_code} trước đó FAILED → tạo record mới")
        return "CONTINUE"

    # 6️ INSERT PROCESS START
    def insert_process_start(self, control_table, process_code, source_id, process_name=""):
        """Tạo record bắt đầu process"""
        sql = f"""
            INSERT INTO {control_table} 
                (process_code, source_id, status, started_at, process_name)
            VALUES (%s, %s, %s, NOW(), %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (process_code, source_id, "PROCESS", process_name))
            self.conn.commit()
            self.process_id = cur.lastrowid
            return self.process_id

    # 7 TÌM FILE MỚI NHẤT    
    def find_latest_files(self, folder_path, pattern):
        """Tìm file mới nhất theo pattern"""
        search_path = os.path.join(folder_path, pattern)
        files = glob.glob(search_path)
        if not files:
            return None
        files.sort(key=os.path.getmtime, reverse=True)
        return files[0]

      # 7.1 Truncate table trước khi load
    def truncate_table(self, table_name):
        """Truncate table trước khi load"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table_name}")
                self.conn.commit()
                print(f"Truncated table: {table_name}")
                return True
        except Exception as e:
            print(f"  WARNING: Error truncating {table_name}: {e}")
            return False
    
    # 8️ LOAD DỮ LIỆU VÀO BẢNG
    def load_csv_to_table(self, csv_file, target_table, truncate_before=False):
        start_time = datetime.now()
        """Load CSV file vào table"""
        try:
            if truncate_before:
                # 7.1 Truncate table trước khi load
                self.truncate_table(target_table)

            print(f"Reading CSV: {os.path.basename(csv_file)}")
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                csv_reader = csv.DictReader(f)
                rows = list(csv_reader)
                
            if not rows:
                print(f"No data in CSV file")
                return False
            
            # Lấy columns từ CSV
            columns = list(rows[0].keys())
            
            # Tạo INSERT statement
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # 7.2 Thêm dữ liệu
            with self.conn.cursor() as cur:
                for row in rows:
                    values = [row[col] for col in columns]
                    cur.execute(sql, values)
                
                self.conn.commit()

                exec_time = (datetime.now() - start_time).total_seconds()
                """ghi file log thành công"""
                self.log_file_success(csv_file, len(rows), exec_time)
                print(f"  ✓ Loaded {len(rows)} rows into {target_table}")
                return True
                
        except Exception as e:
            exec_time = (datetime.now() - start_time).total_seconds()
            """ghi file log thất bại"""
            self.log_file_fail(csv_file, exec_time, status="FAIL")
            print(f"Error loading {csv_file} to {target_table}: {e}")
            return False
    
    def log_file_success(self, csv_file, row_count, exec_time):
        self.write_file_log(
            source_id=self.cfg["jobs"][self.JOB_KEY]["source_id"],
            file_path=csv_file,
            row_count=row_count,
            size=os.path.getsize(csv_file),
            status="SUCCESS",
            execute_time=exec_time
    )
        
    def log_file_fail(self, csv_file, exec_time, status="FAIL"):
        """status có thể FAIL hoặc EMPTY"""
        self.write_file_log(
            source_id=self.cfg["jobs"][self.JOB_KEY]["source_id"],
            file_path=csv_file,
            row_count=0,
            size=os.path.getsize(csv_file),
            status=status,
            execute_time=exec_time
    )    
    # 8 ️ WRITE FILE LOG    
    def write_file_log(self, source_id, file_path, row_count, size, status, execute_time):
        """
        Ghi log file vào bảng file_log
        """

        sql = """
            INSERT INTO file_log (source_id, file_path, time, count, size, status, execute_time)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s)
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (
                    source_id,
                    file_path,
                    row_count,
                    size,
                    status,
                    execute_time
                ))

            self.conn.commit()

            print(f"[FILE LOG] {status} | {os.path.basename(file_path)} | {row_count} rows | {execute_time:.2f}s")

        except Exception as e:
            print(" ERROR writing file_log:", e)

     # 9️ UPDATE SUCCESS STATUS
    def update_process_status(self, control_table, process_id, status, update_time=None):
        """Cập nhật trạng thái process"""
        sql = f"""
            UPDATE {control_table}
            SET status = %s, update_at = %s
            WHERE process_id = %s
        """
        with self.conn.cursor() as cur:
            if update_time is None and status in ("SUCCESS", "FAIL"):
                update_time = datetime.now()
            cur.execute(sql, (status, update_time, process_id))
            self.conn.commit()

    def handle_error(self, control_table, process_code, error_msg, e):
        """Xử lý lỗi và gửi email"""
        print("ERROR in process:", e)
        traceback.print_exc()
        
        try:
            if self.process_id and self.conn:
                self.update_process_status(control_table, self.process_id, "FAIL")
            if self.cfg:
                send_error_email(self.cfg, f"[FAILED] Process {process_code} Failed", error_msg, traceback.format_exc())
        except Exception as email_error:
            print(f"Failed to send error email: {email_error}")

    # 10 CLOSE CONNECTION
    def close_connection(self):
        """Đóng kết nối database"""
        if self.conn and self.conn.open:
            self.conn.close()

    def initialize(self, process_name="", config_source="/D/DW/control/config_load.json"):
        """Khởi tạo config và kết nối database"""
        try:
            """1. Load configuration"""
            self.load_config(config_source)
        except Exception as e:
            print("FATAL ERROR: load_config()", e)
            if self.cfg:
                send_error_email(self.cfg, f"[ERROR] {process_name} Config Load Failed", f"Cannot load config.json: {str(e)}", traceback.format_exc())
            return False

        try:
            """2. Connect to database control"""
            self.get_connection()
        except Exception as e:
            print("FATAL ERROR: DB connection", e)
            send_error_email(self.cfg, f"[ERROR] {process_name} Database Connection Failed", f"Cannot connect to database: {str(e)}", traceback.format_exc())
            return False
        
        return True