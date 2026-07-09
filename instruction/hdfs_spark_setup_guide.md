# Hướng dẫn Cấu hình & Chạy PySpark Kết nối HDFS (Hadoop)

Tài liệu này ghi lại toàn bộ lỗi phát hiện được, cách khắc phục và các bước thiết lập từ đầu đến cuối để chạy thử nghiệm PySpark kết nối cụm HDFS Docker.

---

## 1. Các Vấn đề Gặp phải & Cách Khắc phục

### ❌ Vấn đề 1: Lỗi Xung đột Java trên Windows
* **Hiện tượng:** Khi chạy PySpark trực tiếp trên Windows, chương trình bị crash ngay lập tức với lỗi `java.lang.Error: Enabling a Security Manager is not supported`.
* **Nguyên nhân:** Windows của bạn dùng **Java 24**. Java phiên bản mới đã loại bỏ hoàn toàn tính năng `Security Manager` cũ, trong khi các thư viện xác thực HDFS của Hadoop vẫn gọi hàm này. Nếu hạ xuống **Java 8** thì lại quá cũ so với PySpark 4.1.2.
* **Giải pháp:** Không chạy trực tiếp trên Windows, chuyển sang dùng **WSL2 (Ubuntu)** đã cài sẵn **Java 17** (phiên bản tương thích hoàn hảo) hoặc chạy trực tiếp bằng **Spark Docker Container**.

### ❌ Vấn đề 2: Lỗi Kết nối Mạng (`ConnectTimeoutException`)
* **Hiện tượng:** Chạy PySpark từ WSL kết nối tới HDFS ngoài Docker bị treo 60 giây và báo lỗi Timeout khi kết nối DataNode (`172.21.0.x`).
* **Nguyên nhân:** WSL nằm ngoài lớp mạng ảo của Docker. Khi kết nối tới NameNode, NameNode trả về IP nội bộ của các DataNode (`172.21.0.x`), khiến WSL không thể định tuyến để đọc/ghi dữ liệu thực tế.
* **Giải pháp:** Chạy ứng dụng Spark bằng một **Spark Docker Container** gắn trực tiếp vào mạng nội bộ của cụm Hadoop (`bigdata_default`).

### ❌ Vấn đề 3: Lỗi Tràn Bộ Nhớ (`OutOfMemoryError: Java heap space`)
* **Hiện tượng:** Khi chạy Spark đọc file `games.json` thì bị tràn bộ nhớ JVM và crash.
* **Nguyên nhân:** Định dạng gốc của `games.json` là một Dictionary khổng lồ lồng nhau (Key là ID game). Spark parse file này sẽ coi mỗi ID game là 1 cột, tạo ra bảng dữ liệu có **hơn 50,000 cột** khiến bộ nhớ JVM quá tải.
* **Giải pháp:** 
  1. Chuyển đổi cấu trúc dữ liệu sang dạng **JSON Lines (JSONL)**: mỗi game là 1 dòng, bảng chuyển về cấu trúc chuẩn chỉ có **9 cột** (`app_id`, `name`, `price`,...) và **50,000 dòng**.
  2. File tiền xử lý nằm tại: `preprocess_games.py`

---

## 2. Quy trình Setup và Chạy từ Đầu đến Cuối

### Bước 1: Khởi động các Container HDFS & Jupyter
Tại thư mục gốc dự án (`bigdata`), khởi động các dịch vụ:
```bash
docker-compose up -d --build
```
*(Lệnh này sẽ tải và khởi động NameNode, 2 DataNodes và giao diện Jupyter Notebook `spark-notebook` ở cổng `8888`)*

### Bước 2: Tiền xử lý dữ liệu games.json
Chạy file script Python ở máy local để tạo ra file sạch dạng JSON Lines:
```bash
python preprocess_games.py
```
*(Tệp tin mới sạch hơn sẽ được tạo ra tại: `data/metadata/games_cleaned.json`)*

### Bước 3: Sửa cấu hình upload trong CLI
Mở file `commands/hdfs_cli.py`, sửa dòng **32** để hệ thống upload file sạch lên HDFS:
* **Từ:** `game_path = f"{source_path}metadata/games.json"`
* **Thành:** `game_path = f"{source_path}metadata/games_cleaned.json"`

### Bước 4: Chạy CLI Pipeline gộp và đẩy lên HDFS
Chạy lệnh CLI tích hợp để thực hiện tự động gộp review và đưa lên HDFS:
```bash
# Cài đặt thư viện CLI (nếu chưa cài)
pip install -r requirements.txt

# Chạy pipeline
python cli.py pipeline run
```

### Bước 5: Viết và chạy PySpark trực quan trên Jupyter Notebook
1. Mở trình duyệt truy cập: **`http://localhost:8888`**
2. Nhập Token đăng nhập: **`easy_spark`**
3. Tạo một Notebook mới (chọn `New` -> `Notebook`).
4. Viết code PySpark kết nối tới HDFS và xem kết quả:

```python
from pyspark.sql import SparkSession

# 1. Tạo SparkSession kết nối tới NameNode trong mạng Docker
spark = SparkSession.builder \
    .appName("Steam Games & Reviews Analytics") \
    .master("local[*]") \
    .getOrCreate()

# 2. Đọc file metadata game đã làm sạch từ HDFS
df_games = spark.read.json("hdfs://namenode:9000/steam/metadata/games_cleaned.json")
print("=== DỮ LIỆU GAMES ===")
df_games.select("app_id", "name", "price", "release_date").show(5)

# 3. Đọc dữ liệu review game đã gộp từ HDFS
df_reviews = spark.read.option("header", "true").csv("hdfs://namenode:9000/steam/reviews/merged/merged/*.csv")
print("=== DỮ LIỆU REVIEWS ===")
df_reviews.select("game_id", "review", "voted_up").show(5)
```
Ấn tổ hợp phím **`Shift + Enter`** để chạy trực quan!
