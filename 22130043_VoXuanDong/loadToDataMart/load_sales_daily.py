import sys
from datamart_loader import DataMartLoader

# ============================================================
# LOAD SALES DAILY TO DATA MART - P9
# ============================================================

class LoadSalesDaily(DataMartLoader):
    """Load agg_property_sales_daily to Data Mart"""
    
    def __init__(self):
        super().__init__()
        self.JOB_KEY = "LOAD_SALES_DAILY"
        
    def run(self, config_source="/D/DW/control/config_load.json"):
        if not self.initialize("Load Sales Daily", config_source):
            return False
        
        # 3️ LOAD JOB CONFIG
        job_cfg = self.cfg["jobs"][self.JOB_KEY]
        process_code = job_cfg["process_code"]
        process_name = job_cfg["process_name"]
        source_folder = job_cfg["source_folder"]
        load_tables = job_cfg["load_tables"]
        source_id = job_cfg["source_id"]
        control_table = self.cfg["control_table"]
        depends_on = job_cfg.get("depends_on")

        try:
            # 4️ CHECK DEPENDENCIES
            if depends_on:
                if not self.check_dependencies(control_table, depends_on, process_name):
                    print("ERROR: dependency check failed → STOP")
                    return False

            # 5️ CHECK CURRENT PROCESS
            status = self.check_current_process(control_table, process_code, source_id)
            if status == "SKIP":
                print(f"{process_name} {process_code} already completed today")
                return True

            # 6️ INSERT PROCESS START
            self.insert_process_start(control_table, process_code, source_id, process_name)
            print(f"{process_name} started! process_id = {self.process_id}")

            
            success_count = 0
            for load_cfg in load_tables:
                file_pattern = load_cfg["file_pattern"]
                target_table = load_cfg["target_table"]
                truncate_before = load_cfg.get("truncate_before", False)
                
                print(f"Processing {file_pattern} -> {target_table}")

                # 7 TÌM FILE MỚI NHẤT 
                latest_file = self.find_latest_files(source_folder, file_pattern)
                if not latest_file:
                    print(f"WARNING: No file found matching: {file_pattern}")
                    continue
                
                print(f" Loading file: {latest_file}")
                
                # 8️ LOAD DỮ LIỆU VÀO BẢNG
                if self.load_csv_to_table(latest_file, target_table, truncate_before):
                    success_count += 1
                    print(f"SUCCESS: Successfully loaded {target_table}")
                else:
                    raise Exception(f"Failed to load {target_table}")
            
            # 9️ UPDATE SUCCESS STATUS
            self.update_process_status(control_table, self.process_id, "SUCCESS")
            print(f"DONE: {process_name} completed successfully!")
            return True

        except Exception as e:
            error_msg = f"CRITICAL ERROR: {process_name} {process_code} failed: {str(e)}"
            print(f"ERROR: {error_msg}")
            self.handle_error(control_table, process_code, error_msg, e)
            return False
        finally:
            # 10️ CLOSE CONNECTION
            self.close_connection()

if __name__ == "__main__":
    loader = LoadSalesDaily()
    success = loader.run()
    if not success:
        sys.exit(1)