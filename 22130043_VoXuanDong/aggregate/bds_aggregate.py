import sys
from database_loader import DatabaseLoader

# ============================================================
# BDS AGGREGATE - KẾ THỪA TỪ DATABASE LOADER
# ============================================================

class BDSAggregate(DatabaseLoader):
    """Class xử lý aggregate dữ liệu BĐS"""
    
    def __init__(self):
        super().__init__()
        self.JOB_KEY = "BDS"  # key trong config["jobs"]
        self.PROCESS_CODE = None
        self.AGGREGATE_PROCEDURE = None
        self.PROCESS_NAME = None
    
    def run(self):
        if not self.initialize("BDS"):
                return False
        
        job_cfg = self.cfg["jobs"][self.JOB_KEY]
        self.PROCESS_CODE = job_cfg["process_code"]
        self.AGGREGATE_PROCEDURE = job_cfg["aggregate_procedure"]
        self.PROCESS_NAME = job_cfg["process_name"]
        source_id = job_cfg["source_id"]
        control_table = self.cfg["control_table"]
        depends_on = job_cfg.get("depends_on") 


        try:
            if depends_on:
                if not self.check_dependencies(control_table, depends_on, "BDS Aggregate"):
                    print("ERROR: dependency check failed → STOP")
                    return False

            status = self.check_current_process(control_table, self.PROCESS_CODE, source_id)
            if status == "SKIP":
                print(f"BDS Process {self.PROCESS_CODE} already completed today")
                return True

            self.insert_process_start(control_table, self.PROCESS_CODE, source_id, self.PROCESS_NAME)
            print(f"BDS Process started! process_id = {self.process_id}")
    
            # 4. Chạy aggregate procedures
            if isinstance(self.AGGREGATE_PROCEDURE, list):
                print(f"Running {len(self.AGGREGATE_PROCEDURE)} aggregate procedures...")
                for i, proc in enumerate(self.AGGREGATE_PROCEDURE, 1):
                    print(f"[{i}/{len(self.AGGREGATE_PROCEDURE)}] {proc}")
            else:
                print(f"Running aggregate procedure: {self.AGGREGATE_PROCEDURE}")
            
            self.run_stored_procedure(self.AGGREGATE_PROCEDURE)
            print("All aggregate procedures completed successfully!")

            export_tables = job_cfg.get("export_tables", [])
            if export_tables:
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                print(f"\nExporting {len(export_tables)} tables to files...")
                for i, export_cfg in enumerate(export_tables, 1):
                    table_name = export_cfg["table"]
                    base_filename = export_cfg["file"].replace('.csv', '')
                    file_name = f"{base_filename}_{timestamp}.csv"
                    query = export_cfg["query"]
                    
                    print(f"[{i}/{len(export_tables)}] Exporting {table_name} -> ../data/{file_name}")
                    success = self.export_to_file(query, f"../data/{file_name}")
                    if success:
                        print(f"  SUCCESS: Successfully exported {file_name}")
                    else:
                        print(f"  WARNING: No data to export for {file_name}")
                print("All data export completed!\n")

            self.update_process_status(control_table, self.process_id, "SUCCESS")
            print("DONE: BDS Aggregate completed successfully!")
            return True

        except Exception as e:
            error_msg = f"CRITICAL ERROR: BDS Process {self.PROCESS_CODE} failed: {str(e)}"
            print(f"ERROR: {error_msg}")
            self.handle_error(control_table, self.PROCESS_CODE, error_msg, e)
            return False
        finally:
            self.close_connection()


# ============================================================
# MAIN PROCESS
# ============================================================

if __name__ == "__main__":
    bds_agg = BDSAggregate()
    success = bds_agg.run()
    if not success:
        sys.exit(1)
