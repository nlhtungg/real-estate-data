import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os

BASE_URL = "https://alonhadat.com.vn/nha-dat/can-ban/nha-dat/1/ha-noi/trang--{}.html"
HEADERS = {"User-Agent": "Mozilla/5.0"}

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
    description = safe_select_text(".ct_brief")

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
        "description": description
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
    output_dir = "/opt/airflow/data/raw"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"data-{date_str}.json")

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Đã lưu vào {output_file}")
    except Exception as e:
        print(f"❌ Lỗi khi ghi file: {e}")

if __name__ == "__main__":
    main()