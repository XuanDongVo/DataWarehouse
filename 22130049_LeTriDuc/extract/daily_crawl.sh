#!/bin/bash

# Sử dụng path tương đối (tự động cd vào thư mục chứa script)
cd "$(dirname "$0")"
BASE_DIR="$(pwd)"
DATA_DIR="$BASE_DIR/../data"
ENV_PATH="$BASE_DIR/chotot_env/bin/activate"

# Log file
LOG_FILE="$BASE_DIR/cron_log_$(date +%Y%m%d).txt"
echo "=== Bắt đầu chạy lúc $(date) ===" >> "$LOG_FILE"

# Kiểm tra env tồn tại
if [ ! -f "$ENV_PATH" ]; then
    echo "Lỗi: Env không tồn tại tại $ENV_PATH" >> "$LOG_FILE"
    exit 1
fi

# Kích hoạt env
source "$ENV_PATH" || { echo "Lỗi kích hoạt env" >> "$LOG_FILE"; exit 1; }

# Chạy crawl BDS
echo "Chạy bds.py..." >> "$LOG_FILE"
python bds.py >> "$LOG_FILE" 2>&1 || echo "Lỗi bds.py" >> "$LOG_FILE"

# Chạy crawl ChoTot
echo "Chạy ChoTot.py..." >> "$LOG_FILE"
python ChoTot.py >> "$LOG_FILE" 2>&1 || echo "Lỗi ChoTot.py" >> "$LOG_FILE"

# Xóa file CSV cũ (>1 ngày tuổi) trong data/
echo "Xóa file CSV cũ..." >> "$LOG_FILE"
find "$DATA_DIR" -name "bds_*.csv" -mtime +1 -delete 2>> "$LOG_FILE" || echo "Lỗi xóa bds csv" >> "$LOG_FILE"
find "$DATA_DIR" -name "chotot_*.csv" -mtime +1 -delete 2>> "$LOG_FILE" || echo "Lỗi xóa chotot csv" >> "$LOG_FILE"

# Chain: Chạy load ngay sau crawl nếu thành công
if [ $? -eq 0 ]; then  # $? kiểm tra exit code của lệnh trước (crawl)
    echo "Crawl thành công, bắt đầu chain load..." >> "$LOG_FILE"
    bash daily_load.sh  # Gọi load trực tiếp
else
    echo "Crawl lỗi, bỏ qua load." >> "$LOG_FILE"
fi

echo "=== Hoàn tất crawl lúc $(date) ===" >> "$LOG_FILE"
