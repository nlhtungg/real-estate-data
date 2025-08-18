from pyspark.sql.functions import col, when, split #type: ignore

def clean_rooms_column(df):
    return df.withColumn(
        "rooms_cleaned",
        when((col("rooms").isNull()) | (col("rooms") == ""), -1)
        .otherwise(split(col("rooms"), " ")[0].cast("int"))
    )
