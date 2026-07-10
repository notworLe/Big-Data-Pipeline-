# 🏗️ Phân Tích Toàn Bộ Các Layer HDFS — Big Data Pipeline Steam
> **Why • What • When • How** + Các vấn đề thực tế

---

## 📌 Bức tranh toàn cảnh: Lambda Architecture

```
Steam Dataset (31M reviews CSV + 65k games JSON)
                    │
          ┌─────────▼──────────┐
          │   HDFS Data Lake   │  ← Lưu trữ trung tâm (tất cả mọi thứ đều đi qua đây)
          │  namenode:9000     │
          │  /steam/silver/    │
          └────────┬───────────┘
                   │
       ┌───────────┴────────────┐
       │                        │
  COLD PIPELINE             HOT PIPELINE
  (Batch ETL)               (Streaming)
  etl_batch.py              producer.py → Kafka
       │                        │
  Spark Batch              Spark Streaming
  (7 ngày / lần)           (10s micro-batch)
       │                        │
  Hive Warehouse           MongoDB
  /steam/warehouse/        steam_realtime.*
  (Parquet files)          (Documents)
       │                        │
       └───────────┬────────────┘
                   │
          Django Dashboard
     Tab 1: Lịch sử   Tab 2: Real-time
```

---

## Layer 0: HDFS Infrastructure (Nền tảng lưu trữ)

### ❓ WHY — Tại sao cần HDFS?

**Vấn đề cốt lõi**: 31 triệu review Steam + 65.000 game metadata = hàng chục GB dữ liệu.

| So sánh | Truyền thống (MySQL/pandas) | HDFS |
|---------|--------------------------|------|
| File size giới hạn | ≤ vài GB trước khi chậm | Không giới hạn thực tế |
| Khi ổ cứng hỏng | **Mất toàn bộ dữ liệu** | Tự phục hồi (replication) |
| Xử lý song song | Không (1 CPU, 1 disk) | Có (Spark đọc nhiều node song song) |
| Scale up | Phải thay máy mạnh hơn | Scale out: thêm DataNode |

> **Kết luận**: Với 31M dòng, pandas/MySQL đơn giản là không xử lý được. HDFS là bắt buộc.

---

### 📦 WHAT — HDFS là gì trong project này?

**HDFS (Hadoop Distributed File System)** = một ổ đĩa ảo phân tán, chia dữ liệu ra nhiều máy vật lý.

**Cấu trúc thực tế trong project:**
```
Cluster: 3 node Docker containers
  ┌─────────────────────────────────────────┐
  │  namenode (container)                   │
  │  Port 9870 (Web UI)                     │
  │  Port 9000 (RPC — Spark kết nối vào)    │
  │  Vai trò: "Thư ký" — lưu metadata       │
  │  (file nào, ở block nào, node nào)      │
  └─────────────────────────────────────────┘
         │ chỉ đạo
  ┌──────┴──────┐
  │             │
datanode1    datanode2
(container)  (container)
Vai trò: "Kho" — lưu dữ liệu thật
```

**Cấu trúc thư mục HDFS:**
```
hdfs://namenode:9000/
└── steam/
    ├── silver/                  ← Data Lake (input)
    │   ├── metadata/
    │   │   └── games_lines.jsonl   ← 65k game, 1 game/dòng JSONL
    │   └── reviews/
    │       ├── 2020/part_1.csv     ← ~1M dòng/file (đã merge từ nhiều file nhỏ)
    │       ├── 2021/part_N.csv
    │       └── ...
    ├── warehouse/               ← Data Warehouse (output Batch ETL)
    │   ├── top_games_by_reviews/
    │   │   └── load_date=2026-07-10/  ← Partition theo ngày
    │   ├── sentiment_by_genre/
    │   ├── trend_by_year/
    │   ├── trend_by_month/
    │   └── top_games_by_metadata_ratio/
    └── checkpoints/             ← Fault tolerance cho Streaming
        └── streaming/
```

---

### ⏰ WHEN — Khi nào dùng HDFS?

| Hành động | Thời điểm |
|-----------|-----------|
| Upload dataset lên HDFS | **Một lần duy nhất** khi setup (`python cli.py pipeline run`) |
| Spark đọc từ HDFS | **Mỗi lần** chạy `etl_batch.py` hoặc `streaming_etl.py` |
| Spark ghi Parquet vào HDFS | **Mỗi lần** batch ETL hoàn thành |
| Checkpoint lưu vào HDFS | **Mỗi 10 giây** khi Streaming đang chạy |
| Query qua Hive | **Bất kỳ lúc nào** sau khi ETL đã chạy ít nhất 1 lần |

---

### 🔧 HOW — Cơ chế hoạt động HDFS thực tế

**Replication Factor = 2** (vì chỉ có 2 DataNode)

```
File: games_lines.jsonl (500 MB)
       │
       ▼ HDFS chia thành các Block (mỗi block = 128 MB mặc định)
Block A (128MB) → Copy trên datanode1 VÀ datanode2
Block B (128MB) → Copy trên datanode1 VÀ datanode2
Block C (128MB) → Copy trên datanode1 VÀ datanode2
Block D (116MB) → Copy trên datanode1 VÀ datanode2
```

**Khi Spark đọc**: Spark sẽ chạy task trực tiếp trên node có block đó (**Data Locality**) → không cần copy data qua mạng → nhanh.

**Khi datanode1 chết**: Hadoop tự phát hiện (heartbeat không còn), tự replica dữ liệu từ datanode2 sang đâu đó khác → **Zero downtime**.

---

### ⚠️ CÁC VẤN ĐỀ THỰC TẾ CỦA HDFS LAYER

#### Vấn đề 1: Small Files Problem 🔴
```
Bronze data (gốc): 65.000 file CSV nhỏ (mỗi file 1 app_id)
  → HDFS phải lưu 65.000 block metadata trong NameNode RAM
  → NameNode bị OOM (Out Of Memory)
  → Toàn bộ cluster sập
```
**Giải pháp trong project**: `commands/merge_data.py` gộp nhiều file nhỏ thành `part_1.csv` (~1M dòng/file) trước khi upload.

#### Vấn đề 2: games.json Format Sai 🔴
```json
// Format GỐC (BAD):
{
  "12345": { "name": "Game A", ... },
  "67890": { "name": "Game B", ... }
  ...
}
// → Spark đọc file này sẽ tạo ra 65.000 cột
// → JVM heap overflow → OOM crash
```
**Giải pháp**: `convert_games_jsonl.py` chuyển sang JSON Lines:
```jsonl
{"app_id": "12345", "name": "Game A", ...}
{"app_id": "67890", "name": "Game B", ...}
```
→ Spark chỉ tạo ~9 cột, 65.000 dòng → bình thường.

#### Vấn đề 3: Mạng Docker - WSL không thông 🔴
```
Windows Host → WSL2 → HDFS NameNode (OK, kết nối được)
Windows Host → WSL2 → HDFS DataNode (172.21.0.x) → FAIL
```
**Lý do**: NameNode trả về địa chỉ nội bộ Docker (`172.21.0.x`), WSL không route được.
**Giải pháp**: Chạy Spark **trong container** (`docker exec spark-notebook spark-submit`) → Spark nằm trong cùng Docker network → thấy được DataNode IP.

#### Vấn đề 4: Java Version Conflict 🔴
```
Windows Java 24 → Security Manager bị xóa hoàn toàn
Hadoop libraries → vẫn gọi Security Manager API
→ Crash ngay lúc start: "Enabling a Security Manager is not supported"
```
**Giải pháp**: Dùng Spark container (image jupyter/pyspark-notebook) → có Java 17 tương thích sẵn.

#### Vấn đề 5: Permission denied trên HDFS 🔴
```
Spark chạy với user jovyan
HDFS /steam/ thuộc user root (mặc định)
→ Spark không ghi được vào /steam/warehouse/
→ FileNotFoundException hoặc Permission denied
```
**Giải pháp**: Phải chmod trước:
```bash
docker exec namenode hdfs dfs -chmod -R 777 /steam
```

---

## Layer 1: Cold Pipeline — Batch ETL (`etl_batch.py`)

### ❓ WHY — Tại sao cần Batch ETL?

**Câu hỏi cần trả lời**:
- "Top 10 game được recommend nhiều nhất trong lịch sử Steam?"
- "Genre nào có rating tốt nhất?"
- "Năm 2023 người chơi có happy hơn năm 2022 không?"

→ Những câu hỏi này cần **toàn bộ 31M dòng lịch sử**. Không có tool nào ngoài Spark/Hadoop xử lý được.

---

### 📦 WHAT — Batch ETL làm gì?

**ETL = Extract → Transform → Load**

```
EXTRACT:
  df_games_raw  ← /steam/silver/metadata/games_lines.jsonl (65k game)
  df_reviews_raw ← /steam/silver/reviews/*/*.csv           (~31M review)

TRANSFORM:
  ┌─ Filter recommend hợp lệ ("Recommended" / "Not Recommended")
  ├─ Drop null (game_id, review)
  ├─ Lọc tiếng Anh (ascii_ratio >= 85%)  ← không cần langdetect
  ├─ Label: sentiment = Positive/Negative
  ├─ Label: is_recommended = 1/0
  ├─ Cast: playtime → Integer
  ├─ Parse: post_date → Date ("MMMM d, yyyy" format)
  ├─ Derive: review_year, review_month
  └─ Drop duplicates(game_id, user, review)

JOIN:
  df_reviews_clean ⨝ df_games_clean
  ON game_id = app_id
  HOW = broadcast join (games chỉ 65k dòng → broadcast vào tất cả executors)

AGGREGATE → 5 insight:
  ① top_games_by_reviews          : GROUP BY app_id, COUNT(*), AVG(is_recommended)
  ② sentiment_by_genre            : EXPLODE(genres) → GROUP BY genre
  ③ trend_by_year                 : GROUP BY review_year
  ④ trend_by_month                : GROUP BY review_month
  ⑤ top_games_by_metadata_ratio   : FROM games, positive/(positive+negative)

LOAD:
  → /steam/warehouse/{name}/load_date=YYYY-MM-DD/  (Parquet, APPEND mode)
```

---

### ⏰ WHEN — Khi nào chạy Batch ETL?

| Kịch bản | Tần suất | Trigger |
|----------|----------|---------|
| Demo / kiểm tra | Chạy tay một lần | `spark-submit etl_batch.py` |
| Production thực tế | Mỗi tuần (7 ngày/lần) | Cron job / Airflow |
| Data mới về | Khi có batch CSV mới từ Steam | Sau khi upload HDFS xong |

**Quan trọng**: Mỗi lần chạy tạo **1 partition mới** `load_date=YYYY-MM-DD`. Batch cũ không bao giờ bị xóa (Non-Volatile).

---

### 🔧 HOW — Các kỹ thuật tối ưu trong code

#### 1. Broadcast Join — tránh shuffle 31M dòng
```python
df_joined = df_reviews_clean.join(
    F.broadcast(df_games_clean),   # ← Key: broadcast games (65k dòng, nhẹ)
    df_reviews_clean.game_id == df_games_clean.app_id,
    how="inner"
)
```
**Lý do**: Nếu không broadcast, Spark phải shuffle cả 31M dòng review qua mạng để tìm match → cực chậm, OOM.
Với `broadcast`, games (65k dòng ~50MB) được copy vào RAM của từng executor → join ngay tại chỗ.

#### 2. Giảm shuffle partition
```python
.config("spark.sql.shuffle.partitions", "16")  # mặc định là 200
```
**Lý do**: Trên máy local chỉ có 8-16 core, 200 partition tạo overhead lớn. 16 là đủ.

#### 3. Lọc tiếng Anh không cần thư viện
```python
.withColumn(
    "ascii_ratio",
    F.length(F.regexp_replace(F.col("review"), "[^\\x00-\\x7F]", "")) / 
    F.greatest(F.length(F.col("review")), F.lit(1))
)
.filter(F.col("ascii_ratio") >= 0.85)
```
**Lý do**: `langdetect` không tương thích tốt với Spark distributed mode. Heuristic ASCII đơn giản nhưng hiệu quả: loại được 95%+ review tiếng Hàn/Trung/Nhật/Nga.

#### 4. Non-Volatile APPEND + Partition
```python
def save_append(df, name):
    df.withColumn("load_date", F.lit(BATCH_DATE))
    .write.mode("append")
    .partitionBy("load_date")
    .parquet(f"{OUTPUT_BASE}/{name}")
```
**Kết quả trên HDFS**:
```
/steam/warehouse/top_games_by_reviews/
  load_date=2026-07-09/   ← batch lần 1 (không bao giờ bị xóa)
  load_date=2026-07-10/   ← batch lần 2
  load_date=2026-07-17/   ← batch lần 3
```

---

### ⚠️ CÁC VẤN ĐỀ BATCH ETL

#### Vấn đề 1: OutOfMemoryError: Java heap space 🔴
```bash
# SAI: không đủ RAM
spark-submit etl_batch.py

# ĐÚNG: cấp thêm RAM cho driver
spark-submit --driver-memory 3g etl_batch.py
```
**Lý do cần 3g**: Join 31M review × 65k games, Spark cần buffer kết quả trong driver RAM trước khi ghi.

#### Vấn đề 2: Partition load_date bị trùng khi chạy 2 lần trong 1 ngày
**Không phải lỗi!** → `mode("append")` tạo thêm file Parquet mới trong cùng partition. Hive đọc cả 2 file → aggregate tự nhiên. Chỉ cần để ý khi muốn "chạy lại từ đầu" thì phải xóa partition thủ công.

#### Vấn đề 3: genres là ARRAY không phải STRING
```python
# Phải EXPLODE mảng genres ra từng dòng
.withColumn("genre", F.explode("genres"))
# Nếu không explode → groupBy("genres") ra kết quả vô nghĩa (array as key)
```

---

## Layer 2: Hive Metastore (SQL Interface)

### ❓ WHY — Tại sao cần Hive khi đã có Parquet?

Parquet trên HDFS chỉ là **file** — không có schema, không query được bằng SQL.
Hive là lớp **metadata** (schema registry) ở trên, cho phép query bằng SQL chuẩn.

**Tương tự**: Parquet = ổ cứng ngoài. Hive = File Explorer có tên cột, kiểu dữ liệu.

```sql
-- Không cần biết file Parquet ở đâu, chỉ cần query như SQL bình thường:
SELECT name, ROUND(recommend_rate * 100, 2) AS pct
FROM top_games_by_reviews
WHERE load_date = '2026-07-10'
ORDER BY recommend_rate DESC LIMIT 10;
```

---

### 📦 WHAT — Hive trong project gồm gì?

| Container | Vai trò |
|-----------|---------|
| `hive-metastore-postgresql` | PostgreSQL — lưu metadata (schema, table location) |
| `hive-metastore` | Thrift service — expose metadata cho Spark/Hive |
| `hive-server` | HiveServer2 — nhận SQL query qua JDBC (Beeline, Django) |

**EXTERNAL TABLE** = Hive chỉ đăng ký schema, dữ liệu thật vẫn ở HDFS:
```sql
CREATE EXTERNAL TABLE top_games_by_reviews (
    app_id STRING, name STRING, total_reviews BIGINT, recommend_rate DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/steam/warehouse/top_games_by_reviews';
-- ↑ DROP TABLE chỉ xóa schema trong Hive, dữ liệu HDFS không bị xóa
```

---

## Layer 3: Hot Pipeline — Speed Layer (Streaming)

### ❓ WHY — Tại sao cần Streaming khi đã có Batch?

**Batch ETL** chạy 7 ngày/lần. Trong 7 ngày đó:
- Một game có thể bị **Review Bomb** (hàng nghìn đánh giá tiêu cực trong vài phút)
- Batch không biết gì → không cảnh báo được

**Streaming** xử lý realtime → phát hiện ngay trong 30 giây → ghi alert vào MongoDB → Dashboard hiển thị ngay.

---

### 📦 WHAT — Streaming gồm những gì?

**4 thành phần**:

```
1. PRODUCER (producer.py)
   Đọc CSV → format JSON → gửi vào Kafka topic "steam-reviews" (10 msg/s)

2. KAFKA (docker container)
   Hàng đợi bền vững, 3 partition, tự xử lý nếu consumer chậm

3. SPARK STRUCTURED STREAMING (streaming_etl.py)
   Đọc Kafka → Parse JSON → Window 30s + Watermark 10s
   → Count Positive/Negative → Alert nếu negative > 50
   → Ghi MongoDB mỗi 10 giây (micro-batch)

4. MONGODB (docker container)
   collection: realtime_sentiment  ← tất cả kết quả window
   collection: alerts              ← chỉ window có alert=True
```

---

### 🔧 HOW — Các khái niệm Streaming quan trọng

#### Tumbling Window (Cửa sổ nhảy bậc)
```
Timeline:  13:00:00 ─────────────────────────────────→
                    ┌────────┐┌────────┐┌────────┐
Window:             │  30s   ││  30s   ││  30s   │
                    │00:00   ││00:30   ││01:00   │
                    │ → 00:30││→ 01:00 ││→ 01:30 │
                    └────────┘└────────┘└────────┘

Code: window(col("event_timestamp"), "30 seconds")
```
Mỗi cửa sổ **độc lập**, không overlap → đếm số positive/negative **trong 30 giây đó**.

#### Watermarking (Xử lý data trễ)
```
Scenario:
  - Review gửi lúc 13:00:25 (thuộc window 13:00:00-13:00:30)
  - Nhưng vì lag mạng → Spark nhận lúc 13:00:38 (sau khi window đã đóng!)

Không có watermark:
  → Review bị DROP (mất dữ liệu)

Có watermark 10s:
  .withWatermark("event_timestamp", "10 seconds")
  → Spark giữ window 13:00:00-13:00:30 trong memory thêm 10 giây
  → Tới 13:00:40 Spark mới đóng window thật sự
  → Review lúc 13:00:25 (nhận lúc 13:00:38) → VẪN ĐƯỢC TÍNH ✅
```

#### Anomaly Detection
```python
.withColumn("alert", col("negative") > 50)
# Ngưỡng: 50 reviews tiêu cực trong 30 giây
# Bình thường: ~5-10 negative/30s
# Review Bomb: hàng trăm negative/30s → alert = True
```

#### Micro-batch Trigger
```python
.trigger(processingTime="10 seconds")
# Cứ 10 giây: Spark lấy batch từ Kafka, xử lý, đẩy vào MongoDB
# Không xử lý từng message ngay lập tức (đó là true streaming - phức tạp hơn)
# Micro-batch = cân bằng giữa latency thấp và throughput cao
```

#### Checkpoint trên HDFS
```python
.option("checkpointLocation", "hdfs://namenode:9000/steam/checkpoints/streaming/")
```
**Khi `streaming_etl.py` bị kill / crash**:
```
HDFS Checkpoint lưu:
  - Kafka offset đang đọc đến đâu (offset 4521 partition 0)
  - State của các window chưa đóng
  
Khi restart:
  → Spark đọc checkpoint → tiếp tục từ offset 4521
  → Không xử lý lại từ đầu (tránh duplicate)
  → Không mất dữ liệu
```

---

### ⚠️ CÁC VẤN ĐỀ STREAMING

#### Vấn đề 1: `alerts.count()` trigger Spark job mỗi micro-batch 🟡
```python
# Code hiện tại — có thể chậm:
alerts = batch_df.filter(col("alert") == True)
if alerts.count() > 0:   # ← trigger thêm 1 Spark action!
    alerts.write...

# Tốt hơn:
alerts = batch_df.filter(col("alert") == True)
alerts.write...  # MongoDB connector tự bỏ qua nếu DF empty
```

#### Vấn đề 2: `outputMode("update")` vs `outputMode("complete")`
```
update   → chỉ gửi các window VỪA THAY ĐỔI (tiết kiệm, dùng cho MongoDB)
complete → gửi toàn bộ tất cả window mỗi batch (phí, chỉ dùng cho console debug)
append   → không dùng được với aggregation có watermark (Spark không biết window nào "hoàn chỉnh")
```

#### Vấn đề 3: Kafka dual listener config
```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
#                           ↑ Container-to-Container    ↑ Host (Windows/WSL) to Container
```
- `kafka:9092` → Spark Streaming (trong container) dùng
- `localhost:29092` → producer.py chạy từ máy host dùng

---

## 🔄 Tổng Hợp: Luồng Dữ Liệu End-to-End

```
1. DATA INGESTION (một lần)
   games.json → convert_games_jsonl.py → games_lines.jsonl
   CSV files  → merge_data.py → part_1.csv, part_2.csv...
   Local → python cli.py pipeline run → HDFS /steam/silver/

2. COLD PIPELINE (định kỳ 7 ngày)
   HDFS /steam/silver/ → etl_batch.py (Spark) → HDFS /steam/warehouse/ (Parquet)
   → Hive EXTERNAL TABLE → Django tab 1 query SQL

3. HOT PIPELINE (realtime 24/7)
   CSV (giả lập) → producer.py → Kafka "steam-reviews"
   → streaming_etl.py (Spark Streaming, 10s micro-batch)
   → Window 30s + Watermark 10s → alert nếu negative > 50
   → MongoDB realtime_sentiment + alerts
   → Django tab 2 poll MongoDB mỗi vài giây

4. FAULT TOLERANCE
   HDFS Checkpoint ← Spark Streaming lưu state và offset
   HDFS Replication ← Dữ liệu được nhân bản 2 lần (2 DataNode)
   Kafka Retention ← Giữ messages tối thiểu 7 ngày (mặc định)
```

---

## 📊 4 Tính Chất Data Warehouse (W.H. Inmon)

| Tính chất | Định nghĩa | Bằng chứng trong code |
|-----------|------------|----------------------|
| **Subject-Oriented** | Dữ liệu xoay quanh 1 chủ đề | 5 bảng output đều về "Steam Sentiment Analysis" |
| **Integrated** | Nhiều nguồn → 1 kho nhất quán | JSON + CSV → chuẩn hóa, join, drop duplicate |
| **Time-Variant** | Mọi bản ghi có mốc thời gian | review_year, review_month, load_date partition |
| **Non-Volatile** | Chỉ INSERT, không UPDATE/DELETE | `.write.mode("append").partitionBy("load_date")` |

---

## 🗂️ Tóm tắt toàn bộ Stack

| Container | Image | Vai trò | Port |
|-----------|-------|---------|------|
| namenode | bde2020/hadoop-namenode | HDFS NameNode (metadata) | 9870, 9000 |
| datanode1/2 | bde2020/hadoop-datanode | HDFS DataNode (storage) | — |
| spark-notebook | jupyter/pyspark-notebook | Spark engine + Jupyter UI | 8888 |
| hive-metastore-postgresql | bde2020/hive-metastore-postgresql | Schema store | — |
| hive-metastore | bde2020/hive | Thrift metadata service | 9083 |
| hive-server | bde2020/hive | HiveServer2 SQL endpoint | 10000, 10002 |
| zookeeper | confluentinc/cp-zookeeper | Kafka coordinator | 2181 |
| kafka | confluentinc/cp-kafka | Message queue | 9092, 29092 |
| mongodb | mongo:6.0 | Hot storage NoSQL | 27017 |
