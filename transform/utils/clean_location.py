from pyspark.sql.functions import col, when, regexp_extract, lit, trim # type: ignore

def parse_location(df):
    """Parse location column into road, ward, district, city"""
    try:
        # Tách road từ "đường XXX" hoặc "phố XXX"
        df_with_road = df.withColumn(
            "road",
            when(
                col("location").rlike(r"(đường|phố|Đường|Phố)\s+([^,]+)"),
                trim(regexp_extract(col("location"), r"(?:đường|phố|Đường|Phố)\s+([^,]+)", 1))
            ).otherwise(lit(None))
        )
        
        # Tách ward từ "phường XXX" hoặc "xã XXX"
        df_with_ward = df_with_road.withColumn(
            "ward", 
            when(
                col("location").rlike(r"(phường|xã|Phường|Xã)\s+([^,]+)"),
                trim(regexp_extract(col("location"), r"(?:phường|xã|Phường|Xã)\s+([^,]+)", 1))
            ).otherwise(lit(None))
        )
        
        # Tách district từ "quận XXX" hoặc "huyện XXX"
        df_with_district = df_with_ward.withColumn(
            "district",
            when(
                col("location").rlike(r"(quận|huyện|Quận|Huyện)\s+([^,]+)"),
                trim(regexp_extract(col("location"), r"(?:quận|huyện|Quận|Huyện)\s+([^,]+)", 1))
            ).otherwise(lit(None))
        )
        
        # Thêm cột city với giá trị mặc định "Hà Nội"
        df_with_city = df_with_district.withColumn("city", lit("Hà Nội"))
        
        return df_with_city
        
    except Exception as e:
        print(f"Lỗi khi parse location: {e}")
        return df
