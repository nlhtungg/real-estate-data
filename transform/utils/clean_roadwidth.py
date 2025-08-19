from pyspark.sql.functions import col, when, regexp_replace #type: ignore

def clean_roadwidth_column(df):
    
    try:
        df = df.withColumn(
            "roadwidth_cleaned",
            when((col("road_width").isNull()) | (col("road_width") == ""), -1)
            .otherwise(regexp_replace(col("road_width"), "m$", "").cast("numeric"))
        )

        df = df.drop("road_width").withColumnRenamed("roadwidth_cleaned", "road_width")

    except Exception as e:
        print(f"Error cleaning road width column: {e}")

    return df