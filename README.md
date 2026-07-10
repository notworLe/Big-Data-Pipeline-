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

| Thành phần     | Vai trò                              |
| -------------- | ------------------------------------ |
| HDFS 3.3.6     | Lưu trữ phân tán (Data Lake)         |
| Apache Kafka   | Message queue, giả lập streaming     |
| Apache Spark   | Batch ETL + Structured Streaming     |
| Apache Hive    | Data warehouse cho phân tích lịch sử |
| MongoDB        | Hot storage cho dữ liệu real-time    |
| Django         | Backend API + Dashboard              |
| Docker Compose | Container hóa toàn bộ hệ thống       |

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

| #   | Vai trò                      | Nhiệm vụ chính                                                         |
| --- | ---------------------------- | ---------------------------------------------------------------------- |
| 1   | HDFS Infrastructure          | Docker Compose HDFS, config replication, upload dataset, demo phân tán |
| 2   | Kafka & Streaming Simulation | Setup Kafka/Zookeeper, viết Producer giả lập stream                    |
| 3   | Spark Batch ETL (Cold)       | ETL toàn bộ dataset, schema Hive, aggregate insight lịch sử            |
| 4   | Spark Streaming ETL (Hot)    | Structured Streaming từ Kafka, window + anomaly detection, ghi MongoDB |
| 5   | Analytics & Dashboard        | API Django, dashboard 2 tab (lịch sử / real-time)                      |

---

## Dataset

- **Tên**: Steam Games Metadata and Player Reviews 2020-2024
- **Nguồn**: Mendeley Data — `data.mendeley.com/datasets/jxy85cr3th/2`
- **Quy mô**: ~31 triệu review, hàng chục GB
- **Định dạng**: CSV (review theo từng `app_id`) + JSON (metadata game)

---

## Trạng thái hiện tại

- **Layer 1 — HDFS**: đang cấu hình Docker Compose (NameNode + 2 DataNode), cần review `core-site.xml` / `hdfs-site.xml` và xử lý bài toán small files trước khi ingest dataset.

# Installation

## Yêu cầu

- Docker Desktop (có WSL2 backend)
- Python 3.10+
- Dataset Steam Games từ [Mendeley Data](https://data.mendeley.com/datasets/jxy85cr3th/2)

## Data structure

Tải dataset về và đặt vào thư mục `data/bronze/`:

```
data/
├── bronze/
│   ├── metadata/
│   │   └── games.json          ← file gốc từ dataset
│   └── reviews/
│       ├── 2020/
│       │   ├── 1000_aa.csv
│       │   └── 1000_ab.csv
│       ├── 2021/
│       ├── 2022/
│       ├── 2023/
│       └── 2024/
├── silver/                      ← tự tạo bởi pipeline (bước 2)
│   ├── metadata/
│   │   ├── games.json
│   │   └── games_lines.jsonl   ← tự tạo bởi convert (bước 3)
│   └── reviews/
│       ├── 2020/
│       │   ├── part_1.csv
│       │   └── part_2.csv
│       └── ...
└── gold/
```

---

## Các bước chạy

### Bước 1 — Khởi động Docker cluster

```bash
docker-compose up -d --build
```

Kiểm tra bằng `docker ps`, đảm bảo các container đang chạy: `namenode`, `datanode1`, `datanode2`, `spark-notebook`.

### Bước 2 — Chạy pipeline (transform bronze → setup HDFS → upload lên HDFS)

```bash
python cli.py pipeline run
```

Pipeline sẽ tự động:

1. Merge các file CSV nhỏ trong `bronze/reviews/` thành các part file lớn (~1M rows) vào `silver/reviews/`
2. Copy `games.json` sang `silver/metadata/`
3. Tạo thư mục trên HDFS (`/steam/silver/`)
4. Upload toàn bộ data silver lên HDFS (chỉ upload year/file chưa có, idempotent)

> **Muốn biết rõ**: thêm `--help` để xem các lệnh con.

### Bước 3 — Convert games.json sang JSON Lines (chạy 1 lần duy nhất)

```bash
docker exec spark-notebook python /home/jovyan/work/convert_games_jsonl.py
```

> File `games.json` gốc là 1 JSON object lớn `{ "app_id": {...}, ... }` — Spark không đọc được trực tiếp.
> Script này convert sang JSON Lines (1 game/dòng) để Spark xử lý hiệu quả.

### Bước 4 — Upload file JSONL lên HDFS (chạy 1 lần duy nhất)

```bash
docker exec namenode hdfs dfs -put -f /opt/data/silver/metadata/games_lines.jsonl /steam/silver/metadata/
```

### Bước 5 — Cấp quyền ghi cho Spark trên HDFS (chạy 1 lần duy nhất)

```bash
docker exec namenode hdfs dfs -chmod -R 777 /steam
```

> Spark chạy với user `jovyan`, cần quyền ghi vào `/steam/warehouse/` để lưu kết quả Parquet.

### Bước 6 — Chạy ETL Batch (Cold Pipeline — PySpark)

```bash
docker exec spark-notebook /usr/local/spark/bin/spark-submit --driver-memory 3g /home/jovyan/work/etl_batch.py
```

Kết quả khi thành công:

```
Đang đọc dữ liệu từ HDFS...
Đang làm sạch dữ liệu review...
Đang join reviews với metadata game...
Tính: Top game positive nhất...
Tính: Sentiment theo genre...
Tính: Trend theo năm...
Tính: Trend theo tháng...
Tính: Top game theo sentiment_ratio (positive/negative gốc từ metadata)...
Đang ghi kết quả ra HDFS (Parquet) — batch load_date=YYYY-MM-DD...
=== ETL HOÀN TẤT ===
```

5 bảng Parquet được ghi vào HDFS tại `/steam/warehouse/`:

| Bảng                          | Nội dung                                  |
| ----------------------------- | ----------------------------------------- |
| `top_games_by_reviews`        | Top game positive nhất (≥20 review)       |
| `sentiment_by_genre`          | Tỉ lệ recommend theo genre                |
| `trend_by_year`               | Trend sentiment theo năm                  |
| `trend_by_month`              | Trend sentiment theo tháng                |
| `top_games_by_metadata_ratio` | Top game theo positive/negative ratio gốc |

> ⚠️ `--driver-memory 3g` **bắt buộc** để tránh OutOfMemoryError khi join ~31M dòng review.

---

> **Lưu ý**: Bước 3, 4, 5 chỉ cần chạy **1 lần duy nhất**. Từ lần sau chỉ cần chạy bước 2 (nếu có data mới) và bước 6.

### STREAMING

Terminal 1 — Bật Spark Streaming (Consumer)

```bash
docker exec spark-notebook /usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 /home/jovyan/work/streaming_etl.py
```

(Chờ nó hiện chữ Streaming Pipeline Started. Waiting for data...)

Terminal 2 — Bật Python Producer
```bash
docker exec -w /home/jovyan/work spark-notebook python -c "from producer import send_reviews; send_reviews(rate=10)"
```

Terminal 3 — Xem MongoDB
```bash
docker exec mongodb mongosh steam_realtime --eval "db.realtime_sentiment.find().sort({_id: -1}).limit(3)"
```
