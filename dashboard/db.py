import mysql.connector
import pandas as pd

# CẤU HÌNH KẾT NỐI MYSQL (giống DBeaver)
DB_CONFIG = {
    "host": "146.190.93.160",
    "port": 3308,          # nếu DBeaver dùng port khác thì sửa lại
    "user": "root",
    "password": "123456",  # sửa đúng mật khẩu của bạn
    "database": "mart",
}


def query_df(sql: str) -> pd.DataFrame:
    """
    Chạy câu lệnh SQL và trả về kết quả dạng pandas DataFrame.
    """
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df
