from pyspark.sql.functions import col, when, split #type: ignore

def clean_floors_column(df):
    return df.withColumn(
        "floors_cleaned",
        when((col("floors").isNull()) | (col("floors") == ""), -1)
        .otherwise(split(col("floors"), " ")[0].cast("int"))
    )
