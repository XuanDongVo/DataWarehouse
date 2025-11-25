from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urljoin
from datetime import datetime
import csv
import time
import random
import os

url = "https://batdongsan.com.vn/nha-dat-ban-tp-hcm"
base_url = "https://batdongsan.com.vn/"
max_pages = 2

# Lưu vào thư mục data giống hệt ChoTot.py
output_dir = "../data"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, f"bds_{datetime.now().strftime('%d%m%Y')}.csv")

all_data = []
all_columns = set()

def random_delay(min_s, max_s, desc=""):
    delay = random.uniform(min_s, max_s)
    print(f" Nghỉ {delay:.2f}s {desc}...")
    time.sleep(delay)

def scroll_to_bottom(page, max_scrolls=10):
    print(" Đang cuộn trang để load dữ liệu...")
    for i in range(max_scrolls):
        page.mouse.wheel(0, 3000)
        time.sleep(1.5)
        links = page.query_selector_all("a.js__product-link-for-product-id")
        if len(links) >= 20:
            break
    print(f" Đã cuộn xong, phát hiện {len(links)} tin đăng.")
    return links

with sync_playwright() as p:
    # TRÊN SERVER: dùng headless=True + args chống detect
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--start-maximized",
            "--disable-features=ImproveInformer,TranslateUI",
            "--disable-component-extensions-with-background-pages"
        ]
    )

    for page_num in range(1, max_pages + 1):
        page_url = url if page_num == 1 else f"{url}/p{page_num}"
        print(f"\n=== Đang crawl trang {page_num}: {page_url} ===")

        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
        )

        try:
            page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(8000)  # chờ JS load

            links = scroll_to_bottom(page)
            hrefs = []
            for a in links:
                href = a.get_attribute("href")
                if href:
                    full_url = urljoin(base_url, href)
                    if full_url not in hrefs:
                        hrefs.append(full_url)

            print(f" Thu thập được {len(hrefs)} tin đăng.")

            for idx, link in enumerate(hrefs, 1):
                print(f"  [{page_num}.{idx}] {link}")
                detail_page = browser.new_page(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
                )
                try:
                    detail_page.goto(link, timeout=60000)
                    detail_page.wait_for_selector(".re__pr-specs-content-item-title", timeout=20000)

                    title = detail_page.query_selector("h1.re__pr-title, h1.re__pr-title__value")
                    title_text = title.inner_text().strip() if title else "Không có tiêu đề"

                    titles = detail_page.query_selector_all(".re__pr-specs-content-item-title")
                    values = detail_page.query_selector_all(".re__pr-specs-content-item-value")

                    item = {"Link": link, "Tiêu đề": title_text}
                    for t, v in zip(titles, values):
                        key = t.inner_text().strip()
                        val = v.inner_text().strip()
                        item[key] = val
                        all_columns.add(key)

                    all_data.append(item)
                    print(f"   → OK: {len(item)-2} thuộc tính")

                except Exception as e:
                    print(f"   → LỖI: {e}")
                finally:
                    detail_page.close()

                random_delay(4, 9, "tránh bị chặn")

        except Exception as e:
            print(f" Trang {page_num} lỗi tổng: {e}")
        finally:
            page.close()

        random_delay(12, 25, "chuyển trang")

    browser.close()

# Ghi file CSV
all_columns.discard("Tiêu đề")
columns = ["Link", "Tiêu đề"] + sorted(all_columns)

with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    writer.writerows(all_data)

print(f"\nHOÀN TẤT 100%!\nĐã lưu {len(all_data)} tin vào file:\n{output_file}")