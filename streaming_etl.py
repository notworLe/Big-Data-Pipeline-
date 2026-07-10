from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when
from pyspark.sql.types import StructType, StringType, DoubleType

# Chạy bằng:
# docker exec spark-notebook /usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 /home/jovyan/work/streaming_etl.py

spark = SparkSession.builder \
    .appName("SteamStreamingETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("game_id", StringType()) \
    .add("recommend", StringType()) \
    .add("review_text", StringType()) \
    .add("event_time", DoubleType())

# 1. INGESTION — đọc từ Kafka
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "steam-reviews") \
    .option("startingOffsets", "latest") \
    .load()

# 2. TRANSFORMATION — parse + clean + label
parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

labeled = parsed.withColumn(
    "sentiment",
    when(col("recommend") == "Recommended", "Positive").otherwise("Negative")
).withColumn(
    # event_time is double (seconds), cast to timestamp
    "event_timestamp", col("event_time").cast("timestamp")
)

# Window 30s + watermark 10s (cho phép data trễ tối đa 10s)
aggregated = labeled \
    .withWatermark("event_timestamp", "10 seconds") \
    .groupBy(
        window(col("event_timestamp"), "30 seconds"),
        col("game_id")
    ) \
    .agg(
        count(when(col("sentiment") == "Positive", True)).alias("positive"),
        count(when(col("sentiment") == "Negative", True)).alias("negative")
    ) \
    .withColumn("alert", col("negative") > 50)

# 3. SINK — ghi vào MongoDB qua foreachBatch
def write_to_mongo(batch_df, batch_id):
    # Fix the window column so it can be written to MongoDB (structs are messy in Mongo)
    batch_df = batch_df.withColumn("window_start", col("window.start")).withColumn("window_end", col("window.end")).drop("window")
    
    # Save realtime sentiment
    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017") \
        .option("spark.mongodb.write.database", "steam_realtime") \
        .option("spark.mongodb.write.collection", "realtime_sentiment") \
        .save()

    # Tách riêng alert ra collection khác
    alerts = batch_df.filter(col("alert") == True)
    if alerts.count() > 0:
        alerts.write \
            .format("mongodb") \
            .mode("append") \
            .option("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017") \
            .option("spark.mongodb.write.database", "steam_realtime") \
            .option("spark.mongodb.write.collection", "alerts") \
            .save()

# 4. CHECKPOINT — bắt buộc, lưu trên HDFS
query = aggregated.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("update") \
    .option("checkpointLocation", "hdfs://namenode:9000/steam/checkpoints/streaming/") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming Pipeline Started. Waiting for data...")
query.awaitTermination()
