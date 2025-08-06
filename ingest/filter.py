# /opt/airflow/ingest/merge_filtered.py
import os
import json
from glob import glob
from pathlib import Path

RAW_DIR = "/opt/airflow/data/raw"
FILTERED_DIR = "/opt/airflow/data/filtered"
os.makedirs(FILTERED_DIR, exist_ok=True)

def load_json_files():
    all_data = []
    seen_urls = set()
    for file in sorted(glob(os.path.join(RAW_DIR, "data-*.json"))):
        with open(file, "r", encoding="utf-8") as f:
            try:
                records = json.load(f)
                for rec in records:
                    if rec["url"] not in seen_urls:  # chống trùng lặp
                        all_data.append(rec)
                        seen_urls.add(rec["url"])
            except Exception as e:
                print(f"Lỗi khi đọc {file}: {e}")
    return all_data

def main():
    merged_data = load_json_files()
    output_path = os.path.join(FILTERED_DIR, "data-filtered.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    print(f"✅ Đã ghi {len(merged_data)} bản ghi vào {output_path}")

if __name__ == "__main__":
    main()
