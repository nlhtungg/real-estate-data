from pyspark.sql.functions import desc # type: ignore
from .data_transformer import transform_data
from .data_merger import merge_with_existing_data

def read_raw_data(spark):
    """Đọc tất cả file parquet từ bucket raw, transform và merge với dữ liệu existing dựa trên ID"""
    try:
        # Đường dẫn tới bucket raw trong MinIO
        raw_path = "s3a://raw/"
        
        print(f"Đang đọc dữ liệu từ {raw_path}")
        
        # Đọc tất cả file parquet trong bucket raw
        raw_df = spark.read.parquet(f"{raw_path}*.parquet")
        
        print(f"Tổng số records raw: {raw_df.count()}")
        
        # Transform data
        transformed_df = transform_data(raw_df)

        if transformed_df is None:
            return None
            
        # Merge với dữ liệu existing
        final_df, new_records_count = merge_with_existing_data(spark, transformed_df)
        
        print(f"Tổng số records sau merge: {final_df.count()}")
        print(f"Số records mới được thêm: {new_records_count}")
        
        # Show final result
        print("\nDữ liệu cuối cùng (top 10 records có giá cao nhất):")
        final_df.orderBy(desc("price")).show(10, truncate=True)

        return final_df
        
    except Exception as e:
        print(f"Lỗi khi đọc dữ liệu: {e}")
        return None
