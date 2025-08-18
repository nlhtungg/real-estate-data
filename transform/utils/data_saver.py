from pyspark.sql import SparkSession #type: ignore
from pyspark.sql.functions import col #type: ignore

def configure_iceberg_catalog(spark: SparkSession):
    """
    Configure Iceberg catalog for Spark.

    Args:
        spark (SparkSession): The active Spark session.
    """
    spark.conf.set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.my_catalog.type", "hadoop")
    spark.conf.set("spark.sql.catalog.my_catalog.warehouse", "s3a://cleaned/")

def cast_dataframe_to_table_schema(df, table_name, spark):
    """
    Cast DataFrame columns to match the schema of the Iceberg table.

    Args:
        df (DataFrame): The DataFrame to cast.
        table_name (str): The name of the Iceberg table.
        spark (SparkSession): The active Spark session.

    Returns:
        DataFrame: The casted DataFrame.
    """
    table_schema = spark.sql(f"DESCRIBE TABLE my_catalog.default.{table_name}")
    for row in table_schema.collect():
        column_name = row[0]
        column_type = row[1]
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(column_type))
    return df

def save_to_cleaned_bucket(df, table_name, spark):
    """
    Save the DataFrame to the 'cleaned' bucket on MinIO as an Iceberg table.

    Args:
        df (DataFrame): The DataFrame to save.
        table_name (str): The name of the Iceberg table.
        spark (SparkSession): The active Spark session.
    """
    try:
        print("Cấu hình Iceberg catalog...")
        configure_iceberg_catalog(spark)

        print(f"Kiểm tra và tạo bảng Iceberg nếu chưa tồn tại: {table_name}...")
        schema = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in df.schema.fields])
        spark.sql(f"CREATE TABLE IF NOT EXISTS my_catalog.default.{table_name} ({schema}) USING iceberg")

        print("Ép kiểu dữ liệu của DataFrame để khớp với schema bảng...")
        df = cast_dataframe_to_table_schema(df, table_name, spark)

        print(f"Lưu dữ liệu vào bucket 'cleaned' với bảng {table_name}...")
        df.write.format("iceberg") \
            .mode("overwrite") \
            .save(f"my_catalog.default.{table_name}")
        print("Lưu dữ liệu thành công!")
    except Exception as e:
        print(f"Lỗi khi lưu dữ liệu: {e}")