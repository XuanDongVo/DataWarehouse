import sys
import os
from datetime import datetime

# Import các loader classes
from load_price_trends import LoadPriceTrends
from load_sales_daily import LoadSalesDaily  
from load_features_daily import LoadFeaturesDaily

# ============================================================
# LOAD TO DATA MART - CHẠY TẤT CẢ 3 PROCESSES
# ============================================================

class LoadToDataMart:
    """Class điều phối chạy tất cả 3 processes load data vào Data Mart"""
    
    def __init__(self):
        self.processes = [
            {"name": "Load Price Trends", "class": LoadPriceTrends, "code": "P8"},
            {"name": "Load Sales Daily", "class": LoadSalesDaily, "code": "P9"}, 
            {"name": "Load Features Daily", "class": LoadFeaturesDaily, "code": "P10"}
        ]
    
    def run_all(self):
        """Chạy tất cả 3 processes theo thứ tự"""
        print("=" * 70)
        print("STARTING LOAD TO DATA MART - ALL PROCESSES")
        print("=" * 70)
        start_time = datetime.now()
        
        success_count = 0
        total_count = len(self.processes)
        
        for i, process_info in enumerate(self.processes, 1):
            process_name = process_info["name"]
            process_class = process_info["class"]
            process_code = process_info["code"]
            
            print(f"\n[{i}/{total_count}] Starting {process_name} ({process_code})")
            print("-" * 50)
            
            try:
                # Tạo instance và chạy
                loader = process_class()
                success = loader.run()
                
                if success:
                    success_count += 1
                    print(f"SUCCESS: {process_name} completed successfully!")
                else:
                    print(f"FAILED: {process_name} failed!")
                    
            except Exception as e:
                print(f"ERROR in {process_name}: {str(e)}")
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print("LOAD TO DATA MART SUMMARY")
        print("=" * 70)
        print(f"Successful processes: {success_count}/{total_count}")
        print(f"Total execution time: {duration:.2f} seconds")
        print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if success_count == total_count:
            print("ALL PROCESSES COMPLETED SUCCESSFULLY!")
            return True
        else:
            failed_count = total_count - success_count
            print(f"WARNING: {failed_count} PROCESSES FAILED!")
            return False

# ============================================================
# MAIN PROCESS
# ============================================================

if __name__ == "__main__":
    coordinator = LoadToDataMart()
    success = coordinator.run_all()
    
    if success:
        print("\nLoad To Data Mart finished successfully!")
        sys.exit(0)
    else:
        print("\nLoad To Data Mart finished with errors!")
        sys.exit(1)