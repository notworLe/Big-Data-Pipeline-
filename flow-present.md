# 🧊 Cold Pipeline — PySpark Batch ETL: HDFS → Warehouse

> **File nguồn**: `etl_batch.py`  
> **Vai trò**: Member 3 — xử lý toàn bộ lịch sử review Steam (31M dòng)  
> **Luồng**: HDFS (raw CSV + JSON) → Spark Batch → Parquet (HDFS Warehouse)

---

## 1. Tóm tắt luồng xử lý

```
HDFS (Data Lake)
  ├── /steam/metadata/games_cleaned.json     ← metadata game (65k game)
  └── /steam/reviews/merged/merged/*.csv     ← ~31M dòng review

        ↓  Spark đọc & SELECT cột cần thiết (giảm RAM)

  [EXTRACT]
    df_games_raw   → app_id, name, genres, positive, negative, release_date
    df_reviews_raw → game_id, user, playtime, post_date, review, recommend

        ↓  Làm sạch, lọc, thêm cột phái sinh

  [TRANSFORM — Reviews]
    • Lọc recommend hợp lệ ("Recommended" / "Not Recommended")
    • Loại game_id / review null
    • Lọc ngôn ngữ tiếng Anh (ascii_ratio >= 85%) — không cần langdetect
    • Tạo cột: sentiment (Positive/Negative), is_recommended (1/0)
    • Parse playtime → Integer, post_date → Date
    • Tạo: review_year (int), review_month (yyyy-MM)
    • Loại duplicate theo (game_id, user, review)

  [TRANSFORM — Games]
    • Parse release_year từ release_date
    • Tính game_sentiment_ratio = positive / (positive + negative)

        ↓  Broadcast Join (games nhẹ → join nhanh, ít RAM)

  [JOIN]
    df_joined = df_reviews_clean ⨝ df_games_clean ON game_id = app_id

        ↓  5 aggregation độc lập

  [AGGREGATE]
    ① top_games_by_reviews        → xếp hạng game theo recommend_rate (≥ 20 review)
    ② sentiment_by_genre          → recommend_rate theo từng genre
    ③ trend_by_year               → xu hướng sentiment theo năm
    ④ trend_by_month              → xu hướng sentiment theo tháng
    ⑤ top_games_by_metadata_ratio → xếp hạng game theo positive/negative metadata (≥ 50 vote)

        ↓  APPEND (không ghi đè)

  [LOAD → HDFS Warehouse]
    hdfs://namenode:9000/steam/warehouse/
      ├── top_games_by_reviews/load_date=YYYY-MM-DD/
      ├── sentiment_by_genre/load_date=YYYY-MM-DD/
      ├── trend_by_year/load_date=YYYY-MM-DD/
      ├── trend_by_month/load_date=YYYY-MM-DD/
      └── top_games_by_metadata_ratio/load_date=YYYY-MM-DD/
```

---

## 2. Phân tích 4 tính chất Data Warehouse

### ✅ 2.1 Subject-Oriented — Hướng chủ đề

> **Định nghĩa**: Warehouse tổ chức dữ liệu theo **chủ đề phân tích cụ thể**, không phải theo hệ thống tác nghiệp.

| Bằng chứng trong code | Chi tiết |
|---|---|
| Chỉ SELECT cột cần thiết | df_games_raw.select("app_id","name","genres","positive","negative","release_date") — loại bỏ about_the_game, detailed_description, tags... |
| 5 bảng output rõ chủ đề | top_games_by_reviews, sentiment_by_genre, trend_by_year, trend_by_month, top_games_by_metadata_ratio |
| Xoay quanh 1 chủ đề duy nhất | Toàn bộ insight đều phục vụ mục tiêu phân tích sentiment người chơi Steam |

**Kết luận**: ✅ **Thỏa** — dữ liệu được hướng đến chủ đề "Sentiment Analysis của Steam Reviews", không lưu toàn bộ raw data.

---

### ✅ 2.2 Integrated — Tích hợp

> **Định nghĩa**: Dữ liệu từ nhiều nguồn khác nhau được **chuẩn hóa và hợp nhất** về một định dạng thống nhất.

| Bằng chứng trong code | Chi tiết |
|---|---|
| 2 nguồn dữ liệu độc lập | JSON (games_cleaned.json) + CSV (merged/*.csv) — khác format, khác schema |
| Chuẩn hóa kiểu dữ liệu | playtime.cast(IntegerType()), to_date("post_date", "MMMM d, yyyy") |
| Chuẩn hóa nhãn | "Recommended"/"Not Recommended" → sentiment = "Positive"/"Negative", is_recommended = 1/0 |
| Lọc ngôn ngữ | Loại review không phải tiếng Anh bằng ascii_ratio >= 0.85 |
| JOIN tích hợp 2 nguồn | df_reviews_clean ⨝ df_games_clean ON game_id = app_id |
| Loại duplicate | .dropDuplicates(["game_id", "user", "review"]) |

**Kết luận**: ✅ **Thỏa** — 2 nguồn dữ liệu (CSV review + JSON metadata) được làm sạch, chuẩn hóa và tích hợp thành một kho dữ liệu nhất quán.

---

### ✅ 2.3 Time-Variant — Biến đổi theo thời gian

> **Định nghĩa**: Mỗi bản ghi đều gắn với **mốc thời gian**, cho phép phân tích lịch sử và xu hướng theo thời gian.

| Bằng chứng trong code | Chi tiết |
|---|---|
| post_date → Date | F.to_date("post_date", "MMMM d, yyyy") — gắn mốc thời gian cho từng review |
| review_year | F.year("post_date") — phân tích theo năm |
| review_month | F.date_format("post_date", "yyyy-MM") — phân tích theo tháng |
| release_year | F.year(to_date("release_date", ...)) — năm phát hành game |
| BATCH_DATE = date.today().isoformat() | Mỗi lần chạy gắn nhãn thời điểm load: load_date=2026-07-09 |
| trend_by_year & trend_by_month | 2 bảng aggregation chuyên phân tích xu hướng theo thời gian |
| Partition theo load_date | .partitionBy("load_date") — query được dữ liệu theo từng batch load |

**Kết luận**: ✅ **Thỏa** — dữ liệu có đầy đủ chiều thời gian (ngày review, tháng, năm, ngày load), hỗ trợ phân tích xu hướng lịch sử.

---

### ✅ 2.4 Non-Volatile — Bất biến / Không ghi đè

> **Định nghĩa**: Dữ liệu trong warehouse **chỉ được thêm vào (INSERT)**, không bị sửa hoặc xóa.

| Bằng chứng trong code | Chi tiết |
|---|---|
| mode("append") | .write.mode("append") — không dùng "overwrite" |
| partitionBy("load_date") | Mỗi lần chạy tạo 1 partition mới theo ngày, batch cũ không bị đụng tới |
| Comment trong code | # Non-Volatile: KHÔNG overwrite toàn bộ — mỗi lần chạy chỉ APPEND thêm 1 partition mới |
| BATCH_DATE duy nhất mỗi lần chạy | Mỗi ngày tạo folder riêng: load_date=YYYY-MM-DD/ — không xung đột |

**Kết luận**: ✅ **Thỏa** — thiết kế APPEND + partition theo load_date đảm bảo dữ liệu lịch sử không bao giờ bị ghi đè, đúng chuẩn Non-Volatile.

---

### 📋 Tổng kết 4 tính chất

| Tính chất | Trạng thái | Cơ chế |
|---|---|---|
| Subject-Oriented | ✅ Thỏa | 5 bảng output, mỗi bảng 1 chủ đề phân tích riêng |
| Integrated | ✅ Thỏa | Tích hợp JSON + CSV, chuẩn hóa kiểu, ngôn ngữ, loại trùng |
| Time-Variant | ✅ Thỏa | Cột thời gian đa cấp: review_year, review_month, load_date |
| Non-Volatile | ✅ Thỏa | .write.mode("append").partitionBy("load_date") |

> **Kết luận chung**: Layer PySpark (HDFS → Warehouse) trong `etl_batch.py` **thỏa đủ cả 4 tính chất** của một Data Warehouse theo định nghĩa W.H. Inmon.

---

## 3. Hướng dẫn Setup & Run

### Yêu cầu môi trường

| Công cụ | Phiên bản | Ghi chú |
|---|---|---|
| Docker Desktop | >= 4.x | Cần bật WSL2 backend (Windows) |
| Docker Compose | >= 2.x | Đi kèm Docker Desktop |
| Python | >= 3.9 | Dùng cho CLI và script tiền xử lý |
| RAM máy host | >= 8 GB | Spark driver cần ít nhất 3 GB |

---

### Bước 1 — Clone project & chuẩn bị dataset

```bash
git clone <repo-url>
cd Big-Data-Pipeline-

# Đặt dataset vào thư mục đúng cấu trúc:
# data/
#   metadata/
#     games.json          ← từ Mendeley Data
#   reviews/
#     raw/
#       *.csv             ← review theo từng app_id
```

Dataset gốc: https://data.mendeley.com/datasets/jxy85cr3th/2

---

### Bước 2 — Tiền xử lý metadata (bắt buộc)

```bash
python preprocess_games.py
# Output: data/metadata/games_cleaned.json
```

---

### Bước 3 — Khởi động Docker cluster

```bash
docker-compose up -d
docker ps
```

Các container cần chạy:

| Container | Vai trò | Port |
|---|---|---|
| namenode | HDFS NameNode | 9870 (UI), 9000 (RPC) |
| datanode1 | HDFS DataNode 1 | — |
| datanode2 | HDFS DataNode 2 | — |
| spark-notebook | PySpark (Jupyter) | 8888 |
| hive-metastore-postgresql | PostgreSQL cho Hive | — |
| hive-metastore | Hive Metastore | 9083 |
| hive-server | HiveServer2 (Beeline) | 10000, 10002 |

HDFS UI: http://localhost:9870

---

### Bước 4 — Upload dữ liệu lên HDFS

```bash
pip install -r requirements.txt
python cli.py pipeline run
```

Kết quả trên HDFS:

```
hdfs://namenode:9000/steam/
  ├── metadata/games_cleaned.json
  └── reviews/merged/merged/*.csv
```

---

### Bước 5 — Chạy ETL Batch (Cold Pipeline)

```bash
docker exec spark-notebook /usr/local/spark/bin/spark-submit \
    --driver-memory 3g \
    /home/jovyan/work/etl_batch.py
```

> ⚠️ **Quan trọng**: --driver-memory 3g là bắt buộc để tránh OutOfMemoryError khi join 31M dòng.

Output log khi thành công:

```
Đang đọc dữ liệu từ HDFS...
Đang làm sạch dữ liệu review...
Đang join reviews với metadata game...
Tính: Top game positive nhất...
Tính: Sentiment theo genre...
Tính: Trend theo năm...
Tính: Trend theo tháng...
Tính: Top game theo sentiment_ratio (positive/negative gốc từ metadata)...
Đang ghi kết quả ra HDFS (Parquet) — batch load_date=2026-07-09...
=== ETL HOÀN TẤT ===
```

---

### Bước 6 — Đăng ký schema và query trên Hive

```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
```

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS top_games_by_reviews (
    app_id         STRING,
    name           STRING,
    total_reviews  BIGINT,
    recommend_rate DOUBLE,
    load_date      STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/steam/warehouse/top_games_by_reviews';

SELECT name, total_reviews, ROUND(recommend_rate * 100, 2) AS recommend_pct
FROM top_games_by_reviews
ORDER BY recommend_rate DESC
LIMIT 10;

-- Query theo batch load cụ thể
SELECT * FROM top_games_by_reviews
WHERE load_date = '2026-07-09' LIMIT 5;
```

---

### Bước 7 — Chạy lại batch (Time-Variant / Non-Volatile)

Mỗi lần chạy lại etl_batch.py tạo partition mới, batch cũ không bị xóa:

```
steam/warehouse/top_games_by_reviews/
  ├── load_date=2026-07-09/    ← batch lần 1 (giữ nguyên)
  ├── load_date=2026-07-10/    ← batch lần 2
  └── load_date=2026-07-17/    ← batch lần 3
```

---

## 4. Cấu trúc thư mục dự án

```
Big-Data-Pipeline-/
├── etl_batch.py               ← Cold Pipeline ETL (Member 3)
├── preprocess_games.py        ← Chuyển games.json → JSON Lines
├── cli.py                     ← CLI entry point
├── commands/
│   ├── hdfs_cli.py            ← Upload lên HDFS
│   ├── merge_data.py          ← Gộp file CSV review
│   └── pipeline.py            ← Chạy merge + upload tuần tự
├── docker-compose.yml         ← Toàn bộ infrastructure
├── hadoop-tool/
│   ├── hadoop.env             ← Config HDFS
│   └── hive.env               ← Config Hive
├── data/
│   ├── metadata/games_cleaned.json
│   └── reviews/raw/ + merged/
└── flow-present.md            ← Tài liệu này
```

---

## 5. Troubleshooting thường gặp

| Lỗi | Nguyên nhân | Giải pháp |
|---|---|---|
| OutOfMemoryError | Thiếu RAM cho Spark driver | Thêm --driver-memory 3g khi spark-submit |
| Connection refused: namenode:9000 | Container chưa sẵn sàng | Đợi 30s sau docker-compose up |
| FileNotFoundException trên HDFS | Chưa upload dataset | Chạy python cli.py pipeline run trước |
| Parquet không có trong Hive | Chưa CREATE EXTERNAL TABLE | Thực hiện Bước 6 |
| games.json parse error | File JSON array, không phải JSON Lines | Chạy python preprocess_games.py trước |
| Partition load_date bị trùng | Chạy 2 lần trong 1 ngày | APPEND tạo file mới trong cùng partition — an toàn |
