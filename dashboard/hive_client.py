"""
hive_client.py — Kết nối PyHive → Hive → HDFS Parquet
Cache kết quả 1 giờ vì data lịch sử không thay đổi thường xuyên.
"""

import time
import os


HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))  # 1 hour
# Set USE_MOCK=1 to skip Hive entirely (dev / demo mode)
USE_MOCK  = os.getenv("USE_MOCK", "1") == "1"

_cache: dict = {}


def _query_hive(sql: str) -> list[dict]:
    """Execute SQL on Hive; fall back to mock data if unavailable."""
    if USE_MOCK:
        print(f"[HiveClient] USE_MOCK=1 -> returning mock data")
        return _mock_data(sql)
    try:
        from pyhive import hive
        conn = hive.connect(HIVE_HOST, port=HIVE_PORT, database="default")
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0].split(".")[-1] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"[HiveClient] Hive error ({HIVE_HOST}:{HIVE_PORT}): {e}")
        print("[HiveClient] Using mock data for demo.")
        return _mock_data(sql)



def _get_cached(key: str, query_fn) -> list[dict]:
    """Trả về cache nếu còn hạn, không thì query lại."""
    now = time.time()
    if key not in _cache or (now - _cache[key]["ts"]) > CACHE_TTL:
        print(f"[HiveClient] Cache miss: {key} - querying Hive...")
        _cache[key] = {"data": query_fn(), "ts": now}
        print(f"[HiveClient] Cache updated: {key} ({len(_cache[key]['data'])} rows)")
    return _cache[key]["data"]


def _mock_data(sql: str) -> list[dict]:
    """Mock data dự phòng khi Hive chưa sẵn sàng."""
    sql_lower = sql.lower()
    if "top_games_by_reviews" in sql_lower:
        return [
            {"name": "Counter-Strike 2", "total_reviews": 1250000, "recommend_rate": 0.89},
            {"name": "Dota 2", "total_reviews": 980000, "recommend_rate": 0.83},
            {"name": "Terraria", "total_reviews": 750000, "recommend_rate": 0.97},
            {"name": "Stardew Valley", "total_reviews": 620000, "recommend_rate": 0.98},
            {"name": "Hollow Knight", "total_reviews": 430000, "recommend_rate": 0.96},
            {"name": "Cyberpunk 2077", "total_reviews": 390000, "recommend_rate": 0.78},
            {"name": "Elden Ring", "total_reviews": 370000, "recommend_rate": 0.94},
            {"name": "Valheim", "total_reviews": 310000, "recommend_rate": 0.91},
            {"name": "Hades", "total_reviews": 290000, "recommend_rate": 0.97},
            {"name": "Deep Rock Galactic", "total_reviews": 270000, "recommend_rate": 0.98},
        ]
    if "sentiment_by_genre" in sql_lower:
        return [
            {"genre": "Indie", "total_reviews": 8500000, "recommend_rate": 0.88},
            {"genre": "Action", "total_reviews": 7200000, "recommend_rate": 0.82},
            {"genre": "RPG", "total_reviews": 5100000, "recommend_rate": 0.85},
            {"genre": "Strategy", "total_reviews": 4300000, "recommend_rate": 0.87},
            {"genre": "Adventure", "total_reviews": 3800000, "recommend_rate": 0.86},
            {"genre": "Simulation", "total_reviews": 3200000, "recommend_rate": 0.90},
            {"genre": "Sports", "total_reviews": 2100000, "recommend_rate": 0.75},
            {"genre": "Racing", "total_reviews": 1800000, "recommend_rate": 0.78},
            {"genre": "Horror", "total_reviews": 1500000, "recommend_rate": 0.80},
            {"genre": "Puzzle", "total_reviews": 1200000, "recommend_rate": 0.91},
        ]
    if "trend_by_year" in sql_lower:
        return [
            {"review_year": 2020, "total_reviews": 4200000, "recommend_rate": 0.81},
            {"review_year": 2021, "total_reviews": 5800000, "recommend_rate": 0.83},
            {"review_year": 2022, "total_reviews": 6900000, "recommend_rate": 0.82},
            {"review_year": 2023, "total_reviews": 7800000, "recommend_rate": 0.84},
            {"review_year": 2024, "total_reviews": 6500000, "recommend_rate": 0.85},
        ]
    if "trend_by_month" in sql_lower:
        import random
        months = []
        base = 0.82
        for year in [2023, 2024]:
            for month in range(1, 13):
                months.append({
                    "review_month": f"{year}-{month:02d}",
                    "total_reviews": random.randint(400000, 900000),
                    "recommend_rate": round(base + random.uniform(-0.05, 0.07), 3),
                })
        return sorted(months, key=lambda x: x["review_month"])
    return []


# ── Public API ──────────────────────────────────────────────────────────────

def get_top_games() -> list[dict]:
    return _get_cached("top_games", lambda: _query_hive(
        "SELECT name, total_reviews, recommend_rate "
        "FROM top_games_by_reviews "
        "ORDER BY recommend_rate DESC LIMIT 10"
    ))


def get_sentiment_by_genre() -> list[dict]:
    return _get_cached("sentiment_by_genre", lambda: _query_hive(
        "SELECT genre, total_reviews, recommend_rate "
        "FROM sentiment_by_genre "
        "ORDER BY recommend_rate DESC LIMIT 15"
    ))


def get_trend_by_year() -> list[dict]:
    return _get_cached("trend_by_year", lambda: _query_hive(
        "SELECT review_year, total_reviews, recommend_rate "
        "FROM trend_by_year "
        "ORDER BY review_year"
    ))


def get_trend_by_month() -> list[dict]:
    return _get_cached("trend_by_month", lambda: _query_hive(
        "SELECT review_month, total_reviews, recommend_rate "
        "FROM trend_by_month "
        "ORDER BY review_month DESC LIMIT 24"
    ))


def invalidate_cache():
    """Xóa toàn bộ cache — dùng khi cần force refresh."""
    _cache.clear()
    print("[HiveClient] Cache cleared.")
