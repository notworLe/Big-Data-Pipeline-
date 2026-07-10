# 🖥️ Dashboard Layer — FastAPI + Chart.js + SSE

> **File nguồn**: `dashboard/` (sẽ tạo)
> **Vai trò**: Member 5 — trực quan hóa dữ liệu lịch sử (Hive) và real-time (MongoDB)
> **Luồng**: Hive Warehouse + MongoDB → FastAPI → SSE/REST → Chart.js (Browser)

---

## 1. Kiến trúc tổng thể

```
Browser (HTML + Chart.js)
    │
    ├── Tab 1: Lịch sử (Static)
    │       GET /api/top-games          ─────► PyHive → Hive → HDFS Parquet
    │       GET /api/sentiment-by-genre ─────► PyHive → Hive → HDFS Parquet
    │       GET /api/trend-by-year      ─────► PyHive → Hive → HDFS Parquet
    │       GET /api/trend-by-month     ─────► PyHive → Hive → HDFS Parquet
    │
    └── Tab 2: Real-time (Streaming)
            GET /api/stream/realtime ──────► SSE → pymongo → MongoDB
                                            (push mỗi 10 giây)
```

---

## 2. Lý do chọn FastAPI

| Tiêu chí | Django | FastAPI |
|----------|--------|---------|
| Mục đích chính | Full-stack CRUD web | Serve API thuần |
| Async support | Có nhưng phức tạp | **Native async** |
| Tốc độ | Trung bình | Nhanh nhất trong Python |
| SSE / Streaming | Cần extension | **Built-in StreamingResponse** |
| Độ phức tạp | Cao (ORM, admin, template) | Thấp (chỉ cần route + schema) |
| Phù hợp project | ❌ Quá nặng | ✅ Đúng mục đích |

> **Kết luận**: FastAPI chỉ cần 4-5 route, không cần ORM, không cần template → Django là overkill.

---

## 3. SSE vs WebSocket — Tại sao chọn SSE?

**SSE (Server-Sent Events)** = HTTP connection giữ mở, server đẩy data xuống browser một chiều.

```
Browser ──── 1 request ────► FastAPI
Browser ◄─── JSON push ───── FastAPI (mỗi 10s)
Browser ◄─── JSON push ───── FastAPI
```

**WebSocket** = 2 chiều (browser cũng gửi ngược lên server).

| | SSE | WebSocket |
|---|---|---|
| Hướng | Server → Client (1 chiều) | 2 chiều |
| Khi nào dùng | Dashboard chỉ hiển thị | Chat, game, cần gửi ngược |
| Phức tạp | Thấp | Cao hơn |
| Browser reconnect | **Tự động** | Phải tự code |
| Phù hợp project | ✅ | ❌ Phức tạp không cần thiết |

Dashboard chỉ **nhận và hiển thị** data → SSE là đủ, không cần WebSocket.

---

## 4. Cấu trúc thư mục

```
dashboard/
├── main.py              ← FastAPI app, các route API
├── hive_client.py       ← Kết nối PyHive, cache kết quả
├── mongo_client.py      ← Kết nối MongoDB, query realtime
├── static/
│   ├── index.html       ← UI 2 tab
│   ├── app.js           ← Chart.js + SSE logic
│   └── style.css        ← CSS styling
└── requirements.txt
```

---

## 5. Các API Endpoint

### Tab 1 — Lịch sử (query Hive, trả về 1 lần, cache 1 giờ)

| Method | Endpoint | Nguồn dữ liệu | Mô tả |
|--------|----------|---------------|-------|
| GET | `/api/top-games` | Hive `top_games_by_reviews` | Top 10 game positive nhất |
| GET | `/api/sentiment-by-genre` | Hive `sentiment_by_genre` | Recommend rate theo genre |
| GET | `/api/trend-by-year` | Hive `trend_by_year` | Trend sentiment theo năm |
| GET | `/api/trend-by-month` | Hive `trend_by_month` | Trend sentiment theo tháng |

### Tab 2 — Real-time (SSE, push mỗi 10 giây)

| Method | Endpoint | Nguồn dữ liệu | Mô tả |
|--------|----------|---------------|-------|
| GET | `/api/stream/realtime` | MongoDB `realtime_sentiment` | Push sentiment mới nhất |
| GET | `/api/alerts` | MongoDB `alerts` | Danh sách game bị alert |

---

## 6. Luồng dữ liệu chi tiết

### Tab 1 — Lịch sử

```
User mở Tab 1
    → Browser gọi GET /api/top-games
    → FastAPI kiểm tra cache (TTL 1 giờ)
        ├── Cache còn hạn → trả về ngay (< 1ms)
        └── Cache hết hạn → query Hive (5-30s)
                → PyHive kết nối hive-server:10000
                → SELECT ... FROM top_games_by_reviews LIMIT 10
                → Hive đọc Parquet từ HDFS /steam/warehouse/
                → Trả JSON về FastAPI → cache lại → trả về Browser
    → Chart.js nhận JSON → vẽ Bar Chart
    → Không thay đổi nữa (static)
```

### Tab 2 — Real-time

```
User mở Tab 2
    → Browser tạo EventSource("/api/stream/realtime")
    → FastAPI giữ connection mở, vòng lặp vô hạn:
        while True:
            → pymongo query MongoDB realtime_sentiment (10 docs mới nhất)
            → Format JSON → yield "data: {...}\n\n"
            → Browser nhận → Chart.js update Line Chart
            → await sleep(10)
    → Khi có alert → browser highlight đỏ tên game
```

---

## 7. Vấn đề và giải pháp

### ❌ Vấn đề 1: Hive query chậm (5-30 giây)

**Nguyên nhân**: Hive đọc Parquet từ HDFS, parse schema, scan dữ liệu.

**Giải pháp**: Cache kết quả trong memory, TTL 1 giờ.

```python
# Không query Hive mỗi lần user load trang
# Cache 1 giờ vì data lịch sử không đổi
_cache = {}
def get_cached(key, query_fn, ttl=3600):
    if key not in _cache or time.time() - _cache[key]["ts"] > ttl:
        _cache[key] = {"data": query_fn(), "ts": time.time()}
    return _cache[key]["data"]
```

### ❌ Vấn đề 2: SSE bị proxy/nginx cắt sau 60s

**Nguyên nhân**: Nginx/proxy mặc định timeout HTTP connection sau 60 giây không có traffic.

**Giải pháp**: Gửi comment ping mỗi 30 giây để giữ connection sống.

```python
async def event_generator():
    while True:
        data = query_mongo()
        yield f"data: {json.dumps(data)}\n\n"
        # Ping để giữ connection (browser bỏ qua comment SSE)
        await asyncio.sleep(9)
        yield ": ping\n\n"
        await asyncio.sleep(1)
```

### ❌ Vấn đề 3: PyHive kết nối sai host

**Nguyên nhân**: Khi FastAPI chạy trong Docker, không dùng `localhost`.

```python
# SAI (nếu FastAPI trong Docker):
hive.connect("localhost", port=10000)

# ĐÚNG:
hive.connect("hive-server", port=10000)   # tên container trong docker network
```

### ❌ Vấn đề 4: CORS bị chặn khi frontend và backend khác port

**Nguyên nhân**: Browser chặn request cross-origin theo mặc định.

**Giải pháp**:
```python
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(CORSMiddleware, allow_origins=["*"])
```

### ❌ Vấn đề 5: MongoDB document có `_id` ObjectId không serialize được JSON

**Nguyên nhân**: ObjectId không phải kiểu JSON chuẩn.

```python
# Loại _id khi query:
docs = list(collection.find({}, {"_id": 0}).sort("_id", -1).limit(10))
```

---

## 8. Hướng dẫn chạy

### Bước 1 — Cài thư viện

```bash
pip install fastapi uvicorn pyhive pymongo
```

### Bước 2 — Chạy FastAPI

```bash
# Từ thư mục gốc project
uvicorn dashboard.main:app --reload --host 0.0.0.0 --port 8000
```

### Bước 3 — Mở browser

```
http://localhost:8000
```

> **Yêu cầu**: Các container Docker phải đang chạy (hive-server, mongodb, namenode).

---

## 9. Tóm tắt vì sao stack này phù hợp

| Yêu cầu | Giải pháp |
|---------|-----------|
| Query lịch sử 31M rows | PyHive → Hive → HDFS (cache 1 giờ) |
| Real-time mỗi 10s | SSE → pymongo poll MongoDB |
| Không reload trang | Chart.js update in-place qua SSE |
| Cảnh báo review bomb | `alert: true` field → frontend highlight đỏ |
| Nhẹ, đơn giản | FastAPI (không ORM, không template engine) |
