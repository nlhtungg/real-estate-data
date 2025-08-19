from pyspark.sql.functions import col, when, regexp_extract, regexp_replace, lit # type: ignore
from pyspark.sql.types import DoubleType # type: ignore

def clean_price_column(df):
    """Clean and convert price column to double type (unit: triệu VND)"""
    try:
        # Extract số từ đầu string (trước space đầu tiên)
        df_with_number = df.withColumn(
            "price_number",
            regexp_extract(col("price"), r"^([0-9,\.]+)", 1)
        ).withColumn(
            "price_number_clean",
            regexp_replace(col("price_number"), ",", ".")
        )
        
        # Xử lý theo đuôi của price
        df_with_clean_price = df_with_number.withColumn(
            "price_numeric",
            # Case 1: Kết thúc bằng "tỷ" -> nhân 1000000000
            when(
                col("price").endswith("tỷ"),
                col("price_number_clean").cast(DoubleType()) * 1000000000
            )
            # Case 2: Kết thúc bằng "triệu" (không có /m2) -> nhân 1000000
            .when(
                col("price").endswith("triệu") & ~col("price").contains("/"),
                col("price_number_clean").cast(DoubleType()) * 1000000
            )
            # Case 3: Kết thúc bằng "ngàn" (không có /m2) -> nhân 1000
            .when(
                col("price").endswith("ngàn") & ~col("price").contains("/"),
                col("price_number_clean").cast(DoubleType()) * 1000
            )
            # Case 4: Chứa "triệu" và "/" và "m2" -> nhân area nhân 1000000
            .when(
                col("price").contains("triệu") & col("price").contains("/") & col("price").contains("m2"),
                col("price_number_clean").cast(DoubleType()) * col("area") * 1000000
            )
            # Case 5: Chứa "tỷ" và "/" và "m2" -> logic đặc biệt
            .when(
                col("price").contains("tỷ") & col("price").contains("/") & col("price").contains("m2"),
                when(
                    col("price_number_clean").cast(DoubleType()) >= 2,
                    col("price_number_clean").cast(DoubleType()) * 1000000000  # Không nhân area nếu >= 2 tỷ
                ).otherwise(
                    col("price_number_clean").cast(DoubleType()) * 1000000000 * col("area")  # Nhân area nếu < 2 tỷ
                )
            )
            # Case 6: Chứa "ngàn" và "/" và "m2" -> nhân 1000 * area
            .when(
                col("price").contains("ngàn") & col("price").contains("/") & col("price").contains("m2"),
                col("price_number_clean").cast(DoubleType()) * 1000 * col("area")
            )
            # Default: null
            .otherwise(lit(None))
        )
        
        # Drop records có price_numeric null
        valid_price_df = df_with_clean_price.filter(col("price_numeric").isNotNull())
        
        # Use price_numeric for final output
        final_df = valid_price_df
        
        return final_df
        
    except Exception as e:
        print(f"Lỗi khi clean price: {e}")
        return df
