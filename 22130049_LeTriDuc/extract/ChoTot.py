import requests
import pandas as pd
import os
import time
from datetime import datetime

# KHÔNG CẦN CHUẨN HOÁ GIÁ → BỎ parse_price

# Crawl dữ liệu
data_list = []
for page in range(0, 3):  # Crawl 3 trang (mỗi trang ~20 tin)
    url = (
        f"https://gateway.chotot.com/v1/public/ad-listing?"
        f"cg=1000&limit=20&o={page * 20}&st=s,k&f=p&region_v2=13000&area_v2=13100"
    )
    res = requests.get(url)

    if res.status_code != 200:
        print(f"Lỗi khi gọi API trang {page + 1}")
        continue

    ads = res.json().get("ads", [])
    for ad in ads:
        title = ad.get("subject")
        area = ad.get("area_name")
        price_value = ad.get("price")  # Giá VND gốc từ API
        size = ad.get("size")
        rooms = ad.get("rooms")

        data_list.append({
            "title": title,
            "location": area,
            "area": f"{size} m² - {rooms} PN" if size and rooms else None,
            "size": f"{size} m²" if size else None,
            "rooms": f"{rooms} PN" if rooms else None,
            "sqm_m2": size if size else None,
            "price_VND": price_value,  # CHỈ LẤY GIÁ GỐC
            "province": "thanh pho ho chi minh",
            "country": "vietnam"
        })

    print(f" Đã crawl xong trang {page + 1}")
    time.sleep(1)

# Xuất CSV
df = pd.DataFrame(data_list)

output_dir = "../data"
file_basename = f"chotot_{datetime.now().strftime('%d%m%Y')}.csv"
filename = os.path.join(output_dir, file_basename)

# Đảm bảo thư mục 'data' tồn tại
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

df.to_csv(filename, index=False, encoding="utf-8-sig")
print(f"Đã lưu dữ liệu vào {filename}")
