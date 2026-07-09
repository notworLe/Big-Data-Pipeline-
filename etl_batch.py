"""
etl_batch.py — Cold Pipeline: Spark Batch ETL
Member 3 — xử lý toàn bộ lịch sử review Steam (HDFS -> Spark -> Parquet)

Chạy trong container spark-notebook (cấp thêm RAM để tránh OutOfMemoryError):
    docker exec spark-notebook /usr/local/spark/bin/spark-submit \
        --driver-memory 3g \
        /home/jovyan/work/etl_batch.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import date

HDFS_HOST = "namenode:9000"
GAMES_PATH = f"hdfs://{HDFS_HOST}/steam/metadata/games_cleaned.json"
REVIEWS_PATH = f"hdfs://{HDFS_HOST}/steam/reviews/merged/merged/*.csv"
OUTPUT_BASE = f"hdfs://{HDFS_HOST}/steam/warehouse"
BATCH_DATE = date.today().isoformat()  # VD: "2026-07-09" — mỗi lần chạy 1 batch date riêng


def main():
    spark = (
        SparkSession.builder
        .appName("Steam Cold Pipeline - Batch ETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")  # giảm số partition mặc định (200) cho phù hợp dữ liệu vừa/nhỏ
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. EXTRACT
    # ------------------------------------------------------------------
    print("Đang đọc dữ liệu từ HDFS...")

    # Chỉ lấy các cột thực sự cần cho aggregation — bỏ các cột text/array nặng
    # (about_the_game, detailed_description, short_description, categories,
    #  full_audio_languages, supported_languages, tags) để giảm RAM khi join.
    df_games_raw = (
        spark.read.json(GAMES_PATH)
        .select("app_id", "name", "genres", "positive", "negative", "release_date")
    )

    df_reviews_raw = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(REVIEWS_PATH)
        .select("game_id", "user", "playtime", "post_date", "review", "recommend")
    )

    # ------------------------------------------------------------------
    # 2. TRANSFORM — làm sạch reviews
    # ------------------------------------------------------------------
    print("Đang làm sạch dữ liệu review...")

    df_reviews_clean = (
        df_reviews_raw
        # Loại các dòng bị lệch cột do parse lỗi (recommend phải là 1 trong 2 giá trị hợp lệ)
        .filter(F.col("recommend").isin("Recommended", "Not Recommended"))
        # Loại review rỗng / game_id rỗng
        .filter(F.col("game_id").isNotNull() & F.col("review").isNotNull())
        # Lọc tiếng Anh: heuristic đơn giản — tỉ lệ ký tự ASCII trong review phải >= 85%
        # (không cần thư viện langdetect ngoài, đủ để loại review Hàn/Trung/Nhật/Nga...)
        .withColumn(
            "ascii_ratio",
            F.length(F.regexp_replace(F.col("review"), "[^\\x00-\\x7F]", "")) / F.greatest(F.length(F.col("review")), F.lit(1))
        )
        .filter(F.col("ascii_ratio") >= 0.85)
        .drop("ascii_ratio")
        # Label sentiment dạng text theo đúng yêu cầu đề bài
        .withColumn(
            "sentiment",
            F.when(F.col("recommend") == "Recommended", F.lit("Positive")).otherwise(F.lit("Negative"))
        )
        .withColumn("is_recommended", F.when(F.col("recommend") == "Recommended", 1).otherwise(0))
        .withColumn("playtime", F.col("playtime").cast(IntegerType()))
        .withColumn("post_date", F.to_date("post_date", "MMMM d, yyyy"))
        .withColumn("review_year", F.year("post_date"))
        .withColumn("review_month", F.date_format("post_date", "yyyy-MM"))
        .dropDuplicates(["game_id", "user", "review"])
    )

    # ------------------------------------------------------------------
    # 2b. TRANSFORM — làm sạch games + tính sentiment ratio gốc
    # ------------------------------------------------------------------
    df_games_clean = (
        df_games_raw
        .withColumn("release_year", F.year(F.to_date("release_date", "MMM d, yyyy")))
        .withColumn(
            "game_sentiment_ratio",
            F.when(
                (F.col("positive") + F.col("negative")) > 0,
                F.col("positive") / (F.col("positive") + F.col("negative"))
            ).otherwise(None)
        )
    )

    # ------------------------------------------------------------------
    # 3. JOIN reviews + game metadata (đã bỏ cache() để tránh OOM;
    #    Spark sẽ tính lại theo nhu cầu từng aggregation, chậm hơn 1 chút
    #    nhưng an toàn bộ nhớ hơn nhiều so với giữ toàn bộ trong RAM)
    # ------------------------------------------------------------------
    print("Đang join reviews với metadata game...")

    df_joined = df_reviews_clean.join(
        F.broadcast(df_games_clean),  # games nhẹ (65k dòng, đã bỏ cột nặng) -> broadcast join, nhanh & ít tốn RAM hơn shuffle join
        df_reviews_clean.game_id == df_games_clean.app_id,
        how="inner"
    )

    # ------------------------------------------------------------------
    # 4. AGGREGATE — các insight lịch sử
    # ------------------------------------------------------------------

    # 4.1 Top game positive nhất (dựa trên review thực tế, tối thiểu 20 review để tránh nhiễu)
    print("Tính: Top game positive nhất...")
    top_games = (
        df_joined.groupBy("app_id", "name")
        .agg(
            F.count("*").alias("total_reviews"),
            F.avg("is_recommended").alias("recommend_rate")
        )
        .filter(F.col("total_reviews") >= 20)
        .orderBy(F.desc("recommend_rate"))
    )

    # 4.2 Sentiment theo thể loại (genres) — nổ mảng ra từng dòng 1 genre
    print("Tính: Sentiment theo genre...")
    sentiment_by_genre = (
        df_joined
        .withColumn("genre", F.explode("genres"))
        .groupBy("genre")
        .agg(
            F.count("*").alias("total_reviews"),
            F.avg("is_recommended").alias("recommend_rate")
        )
        .orderBy(F.desc("recommend_rate"))
    )

    # 4.3 Trend sentiment theo năm & theo tháng (dựa trên ngày đăng review)
    print("Tính: Trend theo năm...")
    trend_by_year = (
        df_joined
        .filter(F.col("review_year").isNotNull())
        .groupBy("review_year")
        .agg(
            F.count("*").alias("total_reviews"),
            F.avg("is_recommended").alias("recommend_rate")
        )
        .orderBy("review_year")
    )

    print("Tính: Trend theo tháng...")
    trend_by_month = (
        df_joined
        .filter(F.col("review_month").isNotNull())
        .groupBy("review_month")
        .agg(
            F.count("*").alias("total_reviews"),
            F.avg("is_recommended").alias("recommend_rate")
        )
        .orderBy("review_month")
    )

    # 4.4 (Thay thế "developer rating cao nhất" — dataset không có cột developer)
    #     Dùng thay: Top game theo sentiment_ratio gốc từ metadata (positive/negative)
    print("Tính: Top game theo sentiment_ratio (positive/negative gốc từ metadata)...")
    top_games_by_metadata_ratio = (
        df_games_clean
        .filter((F.col("positive") + F.col("negative")) >= 50)
        .select("app_id", "name", "positive", "negative", "game_sentiment_ratio")
        .orderBy(F.desc("game_sentiment_ratio"))
    )

    # ------------------------------------------------------------------
    # 5. LOAD — ghi kết quả ra Parquet trên HDFS (chuẩn bị cho Hive)
    #    Non-Volatile: KHÔNG overwrite toàn bộ — mỗi lần chạy chỉ APPEND
    #    thêm 1 partition mới theo load_date, dữ liệu batch cũ giữ nguyên,
    #    không bị xoá/sửa.
    # ------------------------------------------------------------------
    print(f"Đang ghi kết quả ra HDFS (Parquet) — batch load_date={BATCH_DATE}...")

    def save_append(df, name):
        (
            df.withColumn("load_date", F.lit(BATCH_DATE))
            .write
            .mode("append")
            .partitionBy("load_date")
            .parquet(f"{OUTPUT_BASE}/{name}")
        )

    save_append(top_games, "top_games_by_reviews")
    save_append(sentiment_by_genre, "sentiment_by_genre")
    save_append(trend_by_year, "trend_by_year")
    save_append(trend_by_month, "trend_by_month")
    save_append(top_games_by_metadata_ratio, "top_games_by_metadata_ratio")

    print("Xem thử kết quả (batch mới nhất):")
    top_games.show(10, truncate=False)
    sentiment_by_genre.show(20, truncate=False)
    trend_by_year.show(30, truncate=False)
    trend_by_month.show(10, truncate=False)
    top_games_by_metadata_ratio.show(10, truncate=False)

    print("=== ETL HOÀN TẤT ===")
    spark.stop()


if __name__ == "__main__":
    main()