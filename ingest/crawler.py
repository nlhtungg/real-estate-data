import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from minio import Minio
import tempfile
import os

BASE_URL = "https://alonhadat.com.vn/nha-dat/can-ban/nha-dat/1/ha-noi/trang--{}.html"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# MinIO configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw"

TODAY = datetime.today().strftime("%Y-%m-%d")

def extract_listing(div):
    def safe_select_text(selector, default=""):
        el = div.select_one(selector)
        return el.get_text(strip=True) if el else default

    a_tag = div.select_one(".ct_title a")
    title = a_tag.get_text(strip=True) if a_tag else ""
    url = "https://alonhadat.com.vn" + a_tag["href"] if a_tag and a_tag.has_attr("href") else ""

    price = safe_select_text(".ct_price")
    area = safe_select_text(".ct_dt")
    dimensions = safe_select_text(".ct_kt")
    direction = safe_select_text(".ct_direct")
    location = ", ".join(a.get_text(strip=True) for a in div.select(".ct_dis a"))
    road_width = next((el.get_text(strip=True) for el in div.select(".characteristics span.road-width")), "")
    floors = next((el.get_text(strip=True) for el in div.select(".characteristics span.floors")), "")
    rooms = next((el.get_text(strip=True) for el in div.select(".characteristics span.bedroom")), "")

    return {
        "title": title,
        "url": url,
        "price": price.replace("Giá:", "").strip(),
        "area": area.replace("Diện tích:", "").strip(),
        "dimensions": dimensions.replace("KT:", "").strip(),
        "direction": direction.replace("Hướng:", "").strip(),
        "location": location,
        "floors": floors,
        "rooms": rooms,
        "road_width": road_width,
        "date": TODAY
    }

def crawl_page(page_num):
    print(f"Đang crawl trang {page_num}")
    response = requests.get(BASE_URL.format(page_num), headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    listings = soup.select("div.content-item.item")
    return [extract_listing(div) for div in listings]

def main():
    all_data = []
    for page in range(1, 100):
        all_data.extend(crawl_page(page))

    date_str = datetime.today().strftime("%Y%m%d")
    filename = f"data-{date_str}.parquet"

    try:
        # Tạo DataFrame và ghi ra file tạm
        df = pd.DataFrame(all_data)
        
        # Tạo file tạm
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            temp_path = tmp_file.name
            df.to_parquet(temp_path, index=False, engine="pyarrow")

        # Kết nối tới MinIO
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # Upload file lên bucket raw
        client.fput_object(
            bucket_name=BUCKET_NAME,
            object_name=filename,
            file_path=temp_path,
            content_type="application/octet-stream"
        )

        # Xóa file tạm
        os.unlink(temp_path)

        print(f"✅ Đã upload {filename} lên MinIO bucket '{BUCKET_NAME}'")
    except Exception as e:
        print(f"❌ Lỗi khi upload file: {e}")

if __name__ == "__main__":
    main()