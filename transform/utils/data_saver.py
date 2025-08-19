from pyspark.sql import SparkSession #type: ignore
from pyspark.sql.functions import col #type: ignore

def configure_iceberg_catalog(spark: SparkSession):
    """Cấu hình Iceberg catalog để sử dụng Hive Metastore và bucket cleaned trên Minio"""
    # Sử dụng catalog default của Iceberg
    spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg.type", "hive")
    spark.conf.set("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
    spark.conf.set("spark.sql.catalog.iceberg.warehouse", "s3a://cleaned/")
    
    # Cấu hình S3a cho Minio (nếu chưa có)
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def check_table_exists(spark: SparkSession, table_name: str) -> bool:
    """Kiểm tra xem table có tồn tại hay không"""
    try:
        spark.sql(f"DESCRIBE TABLE iceberg.default.{table_name}")
        return True
    except Exception:
        return False

def create_table_if_not_exists(spark: SparkSession, df, table_name: str):
    """Tạo table Iceberg nếu chưa tồn tại"""
    if not check_table_exists(spark, table_name):
        # Tạo DDL từ schema của DataFrame
        columns = []
        for field in df.schema.fields:
            column_def = f"{field.name} {field.dataType.simpleString()}"
            columns.append(column_def)
        
        schema_ddl = ", ".join(columns)
        
        # Tạo table với location chỉ định trên bucket cleaned
        create_sql = f"""
        CREATE TABLE iceberg.default.{table_name} (
            {schema_ddl}
        ) USING iceberg
        LOCATION 's3a://cleaned/{table_name}'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        """
        
        spark.sql(create_sql)

def cast_dataframe_to_table_schema(df, table_name, spark):
    """Ép kiểu DataFrame để khớp với schema của table"""
    try:
        table_schema = spark.sql(f"DESCRIBE TABLE iceberg.default.{table_name}")
        for row in table_schema.collect():
            column_name = row[0]
            column_type = row[1]
            # Bỏ qua các dòng metadata
            if column_name.startswith('#') or column_name == '':
                continue
            if column_name in df.columns:
                df = df.withColumn(column_name, col(column_name).cast(column_type))
        return df
    except Exception:
        return df

def save_to_cleaned_bucket(df, table_name="example_table", spark=None):
    """
    Lưu DataFrame vào bucket 'cleaned' trên MinIO dưới dạng Iceberg table.
    
    Args:
        df (DataFrame): DataFrame cần lưu
        table_name (str): Tên table Iceberg (mặc định: example_table)
        spark (SparkSession): Spark session đang hoạt động
    """
    if spark is None:
        raise ValueError("SparkSession không được để trống")
        
    try:
        # Cấu hình Iceberg catalog
        configure_iceberg_catalog(spark)
        
        # Tạo namespace default nếu chưa có
        try:
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.default")
        except Exception:
            pass
        
        # Tạo table nếu chưa tồn tại
        create_table_if_not_exists(spark, df, table_name)
        
        # Ép kiểu dữ liệu để khớp với schema
        df_typed = cast_dataframe_to_table_schema(df, table_name, spark)
        
        # Lưu dữ liệu vào Iceberg table
        df_typed.writeTo(f"iceberg.default.{table_name}") \
                .option("write-audit-publish", "true") \
                .overwritePartitions()
        
        print(f"✓ Đã lưu dữ liệu vào iceberg.default.{table_name}")
            
    except Exception as e:
        print(f"❌ Lỗi khi lưu dữ liệu: {e}")
        raise e