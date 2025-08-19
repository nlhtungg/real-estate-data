from pyspark.sql.functions import col, when, split #type: ignore

def clean_floors_column(df):
    
    try:
        df = df.withColumn(
            "floors_cleaned",
            when((col("floors").isNull()) | (col("floors") == ""), -1)
            .otherwise(split(col("floors"), " ")[0].cast("int"))
        )
        df = df.drop("floors").withColumnRenamed("floors_cleaned", "floors")
    
    except Exception as e:
        print(f"Error cleaning floors column: {e}")

    return df