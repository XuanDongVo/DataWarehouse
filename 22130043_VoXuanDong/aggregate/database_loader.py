import json
import pymysql
from datetime import datetime
import sys
import traceback
from email_utils import send_error_email
import csv
import os

class DatabaseLoader:
    """Base class cho các script aggregate - chứa các phương thức chung"""
    
    def __init__(self):
        self.cfg = None
        self.conn = None
        self.process_id = None
    
    def load_config(self, config_source = "/D/DW/control/config_aggregate.json"):
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
        # 2️ CONNECT DATABASE.CONTROL
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

        """
        Kiểm tra nhiều dependency:
        [
            { "process_code": "P5", "source_id": 1 },
            { "process_code": "P6", "source_id": 2 }
        ]
        """
    def check_dependencies(self, control_table, dependencies, process_name=""):
        # 4️ CHECK DEPENDENCIES
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


    def check_current_process(self, control_table, process_code, source_id):
        # 5️ CHECK CURRENT PROCESS
        """Kiểm tra process hiện tại để tránh duplicate"""
        record_curr = self.load_control_record(control_table, process_code, source_id)
        if record_curr:
            if record_curr["status"] in ("PROCESS", "SUCCESS"):
                print(f"INFO: Process {process_code} đang chạy hoặc đã xong → STOP")
                return "SKIP"
            elif record_curr["status"] == "FAIL":
                print(f"INFO: Process {process_code} trước đó FAILED → tạo record mới")
        return "CONTINUE"
    
    def insert_process_start(self, control_table, process_code, source_id, process_name=""):
        # 6️ TẠO RECORD CHO PROCESS_LOG
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
        
    def run_stored_procedure(self, proc_names):
         # 7️ CHẠY STORED PROCEDURES
        """Chạy stored procedure - có thể là 1 procedure hoặc mảng nhiều procedure"""
        # Nếu là string thì convert thành list
        if isinstance(proc_names, str):
            proc_names = [proc_names]
        
        # Chạy từng procedure trong list
        for proc_name in proc_names:
            try:
                print(f"Executing stored procedure: {proc_name}")
                with self.conn.cursor() as cur:
                    cur.callproc(proc_name)
                    self.conn.commit()
                print(f"Successfully executed: {proc_name}")
            except Exception as e:
                print(f"ERROR executing procedure {proc_name}: {e}")
                raise e
    
    def export_to_file(self, query, output_file=None):
        # 8️ EXPORT DATA
        """Xuất dữ liệu từ database ra file CSV chung cho tất cả aggregate"""
        if not output_file:
            # Tạo tên file mặc định theo process code + ngày
            process_code = getattr(self, "PROCESS_CODE", "AGGREGATE")
            output_file = f"{process_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            if not rows:
                print(f"No data to export for {output_file}")
                return False
            
            # Ghi CSV
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
        
        print(f"Data exported to {output_file}")
        return True
    
    def update_process_status(self, control_table, process_id, status, update_time=None):
           # 9️ CẬP NHẬT LẠI STATUS CỦA PROCESS_LOG
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

    def close_connection(self):
      # 10 CLOSE CONNECTION
        """Đóng kết nối database"""
        if self.conn and self.conn.open:
            self.conn.close()

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


    def initialize(self, process_name="", config_source="/D/DW/control/config_aggregate.json"):
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