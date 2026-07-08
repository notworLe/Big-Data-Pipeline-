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
