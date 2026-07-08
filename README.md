# Big Data Pipeline for Steam Game Sentiment Analysis

## Tóm tắt dự án

Xây dựng một hệ thống Big Data end-to-end phân tích sentiment (cảm xúc) người chơi từ **31 triệu review game trên Steam** (2020-2024), kết hợp song song hai luồng xử lý: **batch (lịch sử)** và **streaming (real-time)**, theo mô hình **Lambda Architecture**.

Toàn bộ hệ thống được container hóa bằng Docker, chạy trên môi trường WSL2/Linux.

---

## Vấn đề đặt ra

- Steam có hơn 50,000 game và 31M+ review — vượt khả năng xử lý của công cụ truyền thống (pandas, MySQL).
- Cần một kiến trúc thật sự phân tán để: lưu trữ dữ liệu lớn, xử lý lịch sử theo lô, và phát hiện xu hướng bất thường (ví dụ: review bomb) theo thời gian thực.

## Mục tiêu

1. Lưu trữ phân tán thật (HDFS, 2 DataNode, có fault tolerance).
2. **Cold Pipeline**: HDFS → Spark Batch → Hive Warehouse — phân tích lịch sử toàn bộ dataset.
3. **Hot Pipeline**: Kafka → Spark Streaming → MongoDB — giả lập luồng review thời gian thực, phát hiện anomaly.
4. Dashboard Django trực quan hóa cả hai nguồn dữ liệu.

---

## Kiến trúc tổng thể (Lambda Architecture)

```
Steam Dataset (31M reviews, Mendeley Data)
                ↓
        HDFS Cluster (NameNode + 2 DataNode)
                ↓
      ┌─────────┴─────────┐
      │                   │
 COLD PIPELINE        HOT PIPELINE
 Spark Batch          Kafka Producer (giả lập stream)
 (chạy định kỳ)             ↓
      │              Spark Streaming (window 30s)
      ↓                     ↓
 Hive Warehouse         MongoDB (hot storage)
 (Parquet)                  │
      └─────────┬───────────┘
                 ↓
        Django Dashboard
    Tab 1: Lịch sử (query Hive)
    Tab 2: Real-time (query MongoDB)
```

---

## Công nghệ sử dụng

| Thành phần | Vai trò |
|---|---|
| HDFS 3.3.6 | Lưu trữ phân tán (Data Lake) |
| Apache Kafka | Message queue, giả lập streaming |
| Apache Spark | Batch ETL + Structured Streaming |
| Apache Hive | Data warehouse cho phân tích lịch sử |
| MongoDB | Hot storage cho dữ liệu real-time |
| Django | Backend API + Dashboard |
| Docker Compose | Container hóa toàn bộ hệ thống |

---

## Hai pipeline — phân biệt vai trò

### Cold Pipeline
- **Mục đích**: phân tích lịch sử toàn bộ 31M review.
- **Trigger**: định kỳ (7 ngày/lần, hoặc chạy tay khi demo).
- **Trả lời được**: top game positive nhất, sentiment theo genre, trend theo năm, developer rating cao nhất.

### Hot Pipeline
- **Mục đích**: phát hiện xu hướng đang xảy ra ngay lúc này.
- **Trigger**: liên tục, window aggregation mỗi 30 giây.
- **Trả lời được**: tốc độ review/phút, sentiment ratio gần nhất, cảnh báo game bị review bomb.

---

## Phân công (5 thành viên)

| # | Vai trò | Nhiệm vụ chính |
|---|---|---|
| 1 | HDFS Infrastructure | Docker Compose HDFS, config replication, upload dataset, demo phân tán |
| 2 | Kafka & Streaming Simulation | Setup Kafka/Zookeeper, viết Producer giả lập stream |
| 3 | Spark Batch ETL (Cold) | ETL toàn bộ dataset, schema Hive, aggregate insight lịch sử |
| 4 | Spark Streaming ETL (Hot) | Structured Streaming từ Kafka, window + anomaly detection, ghi MongoDB |
| 5 | Analytics & Dashboard | API Django, dashboard 2 tab (lịch sử / real-time) |

---

## Dataset

- **Tên**: Steam Games Metadata and Player Reviews 2020-2024
- **Nguồn**: Mendeley Data — `data.mendeley.com/datasets/jxy85cr3th/2`
- **Quy mô**: ~31 triệu review, hàng chục GB
- **Định dạng**: CSV (review theo từng `app_id`) + JSON (metadata game)

---

## Trạng thái hiện tại

- **Layer 1 — HDFS**: đang cấu hình Docker Compose (NameNode + 2 DataNode), cần review `core-site.xml` / `hdfs-site.xml` và xử lý bài toán small files trước khi ingest dataset.


# Installition

 ### BƯỚC 1: Khởi động các Container Docker (HDFS & Jupyter)

  Tại thư mục gốc của dự án ( bigdata ), chạy lệnh sau để khởi động HDFS NameNode, DataNodes và Jupyter Notebook:

    docker-compose up -d --build

  (Hãy kiểm tra bằng lệnh  docker ps  để đảm bảo cả 4 container:  namenode ,  datanode1 ,  datanode2 , và  spark-
  notebook  đều đang chạy ở trạng thái  Up ).
  ──────
  ### BƯỚC 2: Tiền xử lý file Metadata (Tránh lỗi bộ nhớ của Spark)

  Trước khi đưa dữ liệu lên HDFS, chúng ta cần chạy script tiền xử lý để chuyển đổi file  games.json  sang dạng JSON
  Lines để Spark không bị lỗi tràn bộ nhớ ( OutOfMemoryError ).

  1. Chạy file script Python ở máy local (Windows/WSL):
    python preprocess_games.py
    Lệnh này sẽ đọc  data/metadata/games.json  và tạo ra file sạch  data/metadata/games_cleaned.json .
  2. Để hệ thống upload file sạch này thay vì file cũ, bạn hãy mở file hdfs_cli.py và sửa dòng 32:
      • Từ:  game_path = f"{source_path}metadata/games.json"
      • Thành:  game_path = f"{source_path}metadata/games_cleaned.json"

  ──────
  ### BƯỚC 3: Chạy CLI Pipeline để gộp dữ liệu và đẩy lên HDFS

  Đúng như bạn nói, dự án đã có sẵn CLI để tự động hóa toàn bộ việc gộp review và đẩy lên HDFS.

  1. Cài đặt thư viện Typer cho CLI (nếu chưa cài):
    pip install -r requirements.txt

  2. Chạy lệnh pipeline:
    python cli.py pipeline run
    Lệnh này sẽ tự động chạy chuỗi nhiệm vụ:
      • Gộp các file review game thô ở thư mục  raw  thành các file gộp ở thư mục  merged .
      • Tạo cấu trúc thư mục trên HDFS.
      • Upload cả tệp review đã gộp và tệp game metadata đã làm sạch ( games_cleaned.json ) lên HDFS.

  ──────
  ### BƯỚC 4: Viết và chạy phân tích PySpark trên Jupyter Notebook

  Bây giờ dữ liệu đã sẵn sàng trên HDFS, bạn có thể thực hiện code PySpark trực quan:

  1. Mở trình duyệt web truy cập:  http://localhost:8888
  2. Nhập Token đăng nhập:  easy_spark
  3. Tạo một Notebook mới (chọn  New  ->  Notebook ).
  4. Viết code PySpark kết nối tới HDFS và xem kết quả:

    from pyspark.sql import SparkSession

    # 1. Tạo SparkSession kết nối tới NameNode trong mạng Docker
    spark = SparkSession.builder \
        .appName("Steam Games & Reviews Analytics") \
        .master("local[*]") \
        .getOrCreate()

    # 2. Đọc file metadata game từ HDFS
    df_games = spark.read.json("hdfs://namenode:9000/steam/metadata/games_cleaned.json")
    print("=== DỮ LIỆU GAMES ===")
    df_games.select("app_id", "name", "price", "release_date").show(5)

    # 3. Đọc dữ liệu review game đã gộp từ HDFS
    df_reviews = spark.read.option("header", "true").csv("hdfs://namenode:9000/steam/reviews/merged/merged/*.csv")
    print("=== DỮ LIỆU REVIEWS ===")
    df_reviews.select("game_id", "review", "voted_up").show(5)

  Bạn nhấn  Shift + Enter  là đã có thể nhìn thấy dữ liệu từ cả 2 nguồn HDFS hiển thị trực quan ngay trên trình
  duyệt!