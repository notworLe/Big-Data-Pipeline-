import os
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count,
    sum as spark_sum, round as spark_round
)

try:
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("SteamBatchETL") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "false") \
        .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "false") \
        .config("spark.driver.memory", "2g") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("=== Spark Session started ===")

    # Đọc data từ HDFS
    print("=== Reading data from HDFS ===")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("recursiveFileLookup", "true") \
        .option("pathGlobFilter", "*.csv") \
        .csv("hdfs://localhost:9000/steam/raw/reviews/reviews_sample2/")

    print(f"Total rows: {df.count()}")
    df.printSchema()

    # Clean data
    print("=== Cleaning data ===")
    df_clean = df.dropna(subset=["app_id", "voted_up"]) \
                 .filter(col("app_id").isNotNull())

    # Label sentiment
    print("=== Labeling sentiment ===")
    df_sentiment = df_clean.withColumn(
        "sentiment",
        when(col("voted_up") == True, "Positive")
        .otherwise("Negative")
    )

    # Aggregate theo game
    print("=== Aggregating by game ===")
    agg_game = df_sentiment.groupBy("app_id") \
        .agg(
            count("*").alias("total_reviews"),
            spark_sum(when(col("sentiment") == "Positive", 1).otherwise(0)).alias("positive_count"),
            spark_sum(when(col("sentiment") == "Negative", 1).otherwise(0)).alias("negative_count")
        ) \
        .withColumn(
            "positive_ratio",
            spark_round(col("positive_count") / col("total_reviews"), 4)
        )

    # Top 10 game positive nhất
    print("=== Top 10 most positive games ===")
    top10 = agg_game.orderBy(col("positive_ratio").desc()).limit(10)
    top10.show()

    # Lưu kết quả lên HDFS
    print("=== Saving results to HDFS ===")
    agg_game.write.mode("overwrite").parquet(
        "hdfs://localhost:9000/steam/warehouse/agg_sentiment"
    )
    df_sentiment.select(
        "app_id", "voted_up", "sentiment"
    ).write.mode("overwrite").parquet(
        "hdfs://localhost:9000/steam/warehouse/fact_reviews"
    )

    print("=== Done! Results saved to HDFS ===")
    spark.stop()

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()