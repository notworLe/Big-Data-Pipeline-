# Big Data Speed Layer (Streaming) — Hướng Dẫn Chi Tiết

Tài liệu này giải thích chi tiết toàn bộ cơ sở lý thuyết và cách áp dụng thực tế vào hệ thống **Speed Layer** của dự án phân tích Steam Reviews.

---

## 1. Cơ sở lý thuyết: Lambda Architecture & Speed Layer

Hệ thống của chúng ta sử dụng **Lambda Architecture**, một kiến trúc xử lý dữ liệu lớn chia làm 2 luồng song song:
1. **Batch Layer (Cold Pipeline):** Xử lý toàn bộ dữ liệu lịch sử (cả chục GB). Chạy chậm nhưng chính xác tuyệt đối. Đã được cài đặt bằng HDFS + Spark Batch.
2. **Speed Layer (Hot Pipeline / Streaming):** Xử lý dữ liệu đang chảy tới theo thời gian thực (real-time). Mục tiêu là bù đắp khoảng thời gian bị trễ của Batch Layer và phát hiện các sự kiện tức thời.

**Speed Layer** không quan tâm đến dữ liệu của năm ngoái, nó chỉ quan tâm đến dữ liệu của **ngay lúc này**. Trong bài toán Steam, chúng ta dùng nó để phát hiện **Review Bombing** (bị đánh giá tiêu cực hàng loạt) ngay tại thời điểm nó xảy ra.

---

## 2. Bốn thành phần cốt lõi của một hệ thống Streaming

Bất kỳ hệ thống Streaming chuẩn công nghiệp nào cũng phải có đủ 4 thành phần (theo lý thuyết Lec04 của môn học):

### Thành phần 1: Ingestion Sources (Nguồn nạp dữ liệu)
- **Lý thuyết:** Dữ liệu streaming không có điểm kết thúc (unbounded). Cần một hệ thống hàng đợi (Message Queue) để hứng dữ liệu tốc độ cao mà không bị mất mát.
- **Thực hành:** Chúng ta sử dụng **Apache Kafka**.
  - File `producer.py` đóng vai trò giả lập một nguồn sinh dữ liệu (như người dùng đang bấm submit review trên web). Nó nạp từng dòng review vào Kafka topic `steam-reviews`.

### Thành phần 2: Transformation Logic (Xử lý và Biến đổi)
- **Lý thuyết:** Dữ liệu sau khi hứng cần được làm sạch, trích xuất và tính toán (aggregate) ngay trên luồng chảy.
- **Thực hành:** Sử dụng **Spark Structured Streaming** (`streaming_etl.py`).
  - *Parse JSON:* Chuyển dữ liệu thô từ Kafka thành các cột có cấu trúc.
  - *Gắn nhãn (Labeling):* Đổi `Recommended` thành `Positive` và `Not Recommended` thành `Negative`.
  - *Window Aggregation:* Đếm số lượng Positive/Negative theo từng cửa sổ thời gian (Window).

### Thành phần 3: Sinks (Đích lưu trữ)
- **Lý thuyết:** Sau khi tính toán, kết quả cần được xuất ra ngoài để frontend hoặc hệ thống cảnh báo có thể đọc được ngay lập tức. Đích đến này phải hỗ trợ ghi/đọc cực nhanh.
- **Thực hành:** Chúng ta sử dụng **MongoDB** (NoSQL Document DB).
  - Dữ liệu sentiment được ghi liên tục vào collection `realtime_sentiment`.
  - Nếu phát hiện bất thường, dữ liệu được rẽ nhánh ghi thêm vào collection `alerts`.

### Thành phần 4: Checkpointing (Lưu trạng thái)
- **Lý thuyết:** Streaming chạy 24/7. Nếu server sập, khi bật lại, hệ thống phải biết nó đang xử lý đến đâu để không xử lý lại từ đầu hoặc bị mất dữ liệu (Fault Tolerance).
- **Thực hành:** Spark lưu *offset* của Kafka và trạng thái tính toán vào **HDFS** (`hdfs://namenode:9000/steam/checkpoints/streaming/`). Khi bật lại `streaming_etl.py`, Spark sẽ tự vào HDFS đọc checkpoint và chạy tiếp từ dòng data bị đứt đoạn.

---

## 3. Các khái niệm nâng cao trong Streaming (Áp dụng trong Code)

### 3.1. Windowing (Cửa sổ thời gian)
Trong dữ liệu luồng, chúng ta không thể "tính tổng tất cả từ trước đến nay" vì dữ liệu là vô hạn. Phải chia thời gian thành các "cửa sổ".
Trong code: `window(col("event_timestamp"), "30 seconds")`
- Chúng ta dùng **Tumbling Window** (Cửa sổ nhảy bậc): 13:00:00 - 13:00:30, 13:00:30 - 13:01:00...
- Giúp đếm được: "Trong 30 giây qua, game A có bao nhiêu đánh giá tích cực/tiêu cực?".

### 3.2. Watermarking (Xử lý dữ liệu đến trễ)
Dữ liệu gửi từ client qua mạng có thể bị lag. Nếu cửa sổ 30s đã đóng mà data mới mò tới thì sao?
Trong code: `.withWatermark("event_timestamp", "10 seconds")`
- Spark sẽ **chờ thêm 10 giây**.
- Ví dụ: Cửa sổ 13:00:00 - 13:00:30 kết thúc. Spark vẫn giữ cửa sổ này trong bộ nhớ tới 13:00:40. Nếu có gói tin bị lag từ 13:00:25 bay tới lúc 13:00:35, Spark vẫn cập nhật nó vào cửa sổ cũ một cách chính xác.

### 3.3. Anomaly Detection (Phát hiện bất thường)
Đây là giá trị cốt lõi nhất của Speed Layer trong bài toán này.
Trong code: `.withColumn("alert", col("negative") > 50)`
- Sau khi gom nhóm đếm số negative trong 30 giây, ta thêm logic so sánh.
- Ngưỡng thiết lập: **50 reviews tiêu cực trong 30s**. Nếu vượt ngưỡng -> Cờ `alert` bật thành `True`.

### 3.4. Trigger & Output Mode
Trong code: 
- `outputMode("update")`: Mỗi khi có dữ liệu mới làm thay đổi kết quả của một Window, Spark sẽ bắn bản ghi mới nhất của Window đó ra ngoài (đẩy vào MongoDB).
- `trigger(processingTime="10 seconds")`: Cứ 10 giây (Micro-batch), Spark lại lấy data từ Kafka vào xử lý một lần.

---

## 4. Tóm tắt Luồng Chạy Thực Tế (Data Flow)

1. **Producer (`producer.py`)**: Đọc từng dòng CSV, ép vào định dạng JSON `{game_id, recommend, event_time}`. Gửi vào Kafka topic `steam-reviews` (Mô phỏng 10 messages / giây).
2. **Kafka (`docker container`)**: Nhận và xếp hàng các JSON messages vào 3 partitions. Đảm bảo data không bị mất mát.
3. **Spark Streaming (`streaming_etl.py`)**: 
   - Đọc JSON từ Kafka.
   - Ép kiểu `event_time` (Unix timestamp float) về kiểu `Timestamp` chuẩn.
   - Nhóm dữ liệu theo `game_id` và cửa sổ `30 giây` (kết hợp Watermark 10 giây).
   - Đếm Positive / Negative. Kiểm tra điều kiện `negative > 50` để bật `alert`.
4. **MongoDB (`docker container`)**: 
   - Nhận kết quả từ Spark đẩy sang mỗi 10 giây (Micro-batch).
   - Lưu trữ dạng Document NoSQL vào 2 collection (`realtime_sentiment` và `alerts`), phục vụ cho Backend API và Dashboard.
