from pyspark.sql.functions import col, when, split #type: ignore

def clean_rooms_column(df):
    
    try:
        df = df.withColumn(
            "rooms_cleaned",
            when((col("rooms").isNull()) | (col("rooms") == ""), -1)
            .otherwise(split(col("rooms"), " ")[0].cast("int"))
        )

        df = df.drop("rooms").withColumnRenamed("rooms_cleaned", "rooms")

    except Exception as e:
        print(f"Error cleaning rooms column: {e}")

    return df