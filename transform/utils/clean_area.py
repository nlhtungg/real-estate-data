from pyspark.sql.functions import col, when, regexp_replace, lit # type: ignore
from pyspark.sql.types import DoubleType # type: ignore

def clean_area_column(df):
    """Clean area column by removing 'm2' suffix and convert to double type"""
    try:
        # Xóa "m2" ở cuối
        df_without_m2 = df.withColumn(
            "area_clean",
            regexp_replace(col("area"), r"\s*m2\s*$", "")
        )
        
        # Chuyển thành double, sử dụng try-catch trong when
        df_with_clean_area = df_without_m2.withColumn(
            "area_numeric",
            when(
                (col("area_clean").isNotNull()) & 
                (col("area_clean") != "") &
                (col("area_clean").rlike(r"^\s*[0-9]+\.?[0-9]*\s*$")),
                col("area_clean").cast(DoubleType())
            ).otherwise(lit(None))
        )
        
        # Drop các dòng có area_numeric là null
        valid_area_df = df_with_clean_area.filter(col("area_numeric").isNotNull())
        
        # Drop các cột tạm và rename
        final_df = valid_area_df.drop("area", "area_clean").withColumnRenamed("area_numeric", "area")
        
        return final_df
        
    except Exception as e:
        print(f"Lỗi khi clean area: {e}")
        return df
