def merge_with_existing_data(spark, new_df, existing_path="s3a://cleaned/default/example_table/data/"):
    """Merge new data with existing cleaned data, avoiding duplicates based on ID"""
    try:
        # Kiểm tra xem có dữ liệu existing không
        try:
            existing_df = spark.read.parquet(f"{existing_path}*.parquet")
            
            # Lấy danh sách IDs đã tồn tại
            existing_ids = existing_df.select("id").distinct()
            
            # Chỉ lấy những records mới có ID chưa tồn tại
            new_records = new_df.join(existing_ids, on="id", how="left_anti")
            
            # Merge dữ liệu mới với dữ liệu cũ (giữ nguyên tất cả thông tin từ raw data)
            merged_df = existing_df.union(new_records)
            
            return merged_df, new_records.count()
            
        except Exception:
            # Nếu không có dữ liệu existing, trả về toàn bộ new_df
            return new_df, new_df.count()
            
    except Exception as e:
        print(f"Lỗi khi merge dữ liệu: {e}")
        return new_df, 0
