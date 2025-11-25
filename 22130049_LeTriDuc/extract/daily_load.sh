#!/bin/bash

# Sử dụng path tương đối
cd "$(dirname "$0")"
BASE_DIR="$(pwd)"
ENV_PATH="$BASE_DIR/chotot_env/bin/activate"
LOG_FILE="$BASE_DIR/cron_log_$(date +%Y%m%d).txt"

echo "=== Bắt đầu load lúc $(date) ===" >> "$LOG_FILE"

# Kiểm tra env
if [ ! -f "$ENV_PATH" ]; then
    echo "Lỗi: Env không tồn tại tại $ENV_PATH" >> "$LOG_FILE"
    exit 1
fi

# Kích hoạt env
source "$ENV_PATH" || { echo "Lỗi kích hoạt env" >> "$LOG_FILE"; exit 1; }

# Chạy load
echo "Chạy load_csv.py..." >> "$LOG_FILE"
python load_csv.py >> "$LOG_FILE" 2>&1 || echo "Lỗi load_csv.py" >> "$LOG_FILE"

echo "=== Hoàn tất load lúc $(date) ===" >> "$LOG_FILE"