from pyspark.sql.functions import col, when, regexp_replace #type: ignore

def clean_roadwidth_column(df):
    return df.withColumn(
        "roadwidth_cleaned",
        when((col("road_width").isNull()) | (col("road_width") == ""), -1)
        .otherwise(regexp_replace(col("road_width"), "m$", "").cast("numeric"))
    )
