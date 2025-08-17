from pyspark.sql.functions import col, hash # type: ignore
from pyspark.sql.types import DoubleType # type: ignore
from .location_parser import parse_location
from .area_cleaner import clean_area_column
from .price_cleaner import clean_price_column

def transform_data(df):
    """Transform raw data to final output format with URL-based deduplication"""
    try:
        # Loại bỏ các records có URL null hoặc empty
        clean_df = df.filter(
            col("url").isNotNull() & 
            (col("url") != "")
        )
        
        # Parse location thành các cột riêng biệt
        parsed_df = parse_location(clean_df)
        
        # Clean area column và drop invalid records
        area_cleaned_df = clean_area_column(parsed_df)
        
        # Clean price column và drop invalid records
        price_cleaned_df = clean_price_column(area_cleaned_df)
        
        # Loại bỏ duplicate URLs, giữ lại record đầu tiên
        deduplicated_df = price_cleaned_df.dropDuplicates(["url"])
        
        # Tạo ID từ hash của URL và select các cột cần thiết
        transformed_df = deduplicated_df.select(
            hash(col("url")).alias("id"),
            col("url"),
            col("road"),
            col("ward"), 
            col("district"),
            col("city"),
            col("area").cast(DoubleType()),
            col("dimensions"),
            col("direction"),
            col("floors"),
            col("rooms"),
            col("road_width"),
            col("price_numeric").cast(DoubleType()).alias("price")  # Sử dụng price_numeric thay vì price
        )
        
        return transformed_df
        
    except Exception as e:
        print(f"Lỗi khi transform dữ liệu: {e}")
        return None
