"""
mongo_client.py — Kết nối MongoDB để lấy dữ liệu real-time từ Speed Layer.
Collections:
  - steam_realtime.realtime_sentiment  ← Spark Streaming ghi vào
  - steam_realtime.alerts              ← Spark Streaming ghi khi negative > 50
"""

import os
import random
import time

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "steam_realtime"

_client = None


def _get_client():
    global _client
    if _client is None:
        try:
            from pymongo import MongoClient
            _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
            _client.admin.command("ping")
            print(f"[MongoClient] Connected to MongoDB: {MONGO_URI}")
        except Exception as e:
            print(f"[MongoClient] Cannot connect to MongoDB: {e}")
            print("[MongoClient] Using mock data for demo.")
            _client = None
    return _client


def get_realtime_sentiment(limit: int = 20) -> list[dict]:
    """Lấy N bản ghi sentiment mới nhất từ MongoDB."""
    client = _get_client()
    if client is None:
        return _mock_sentiment()

    try:
        col = client[DB_NAME]["realtime_sentiment"]
        docs = list(
            col.find({}, {"_id": 0})
            .sort("_id", -1)
            .limit(limit)
        )
        return docs
    except Exception as e:
        print(f"[MongoClient] Query error: {e}")
        return _mock_sentiment()


def get_alerts(limit: int = 10) -> list[dict]:
    """Lấy danh sách alert (game bị review bomb) mới nhất."""
    client = _get_client()
    if client is None:
        return _mock_alerts()

    try:
        col = client[DB_NAME]["alerts"]
        docs = list(
            col.find({}, {"_id": 0})
            .sort("_id", -1)
            .limit(limit)
        )
        return docs
    except Exception as e:
        print(f"[MongoClient] Alerts query error: {e}")
        return _mock_alerts()


# ── Mock data dự phòng ──────────────────────────────────────────────────────

_MOCK_GAMES = [
    ("730", "Counter-Strike 2"),
    ("570", "Dota 2"),
    ("440", "Team Fortress 2"),
    ("1091500", "Cyberpunk 2077"),
    ("1245620", "Elden Ring"),
]

_mock_state: dict = {}


def _mock_sentiment() -> list[dict]:
    """Mock sentiment per window 30s - fresh counts per call."""
    now = time.time()
    results = []
    for game_id, name in _MOCK_GAMES:
        positive = random.randint(5, 30)
        negative = random.randint(1, 25)
        # Cyberpunk 2077 occasionally triggers alert for demo
        if game_id == "1091500" and random.random() < 0.3:
            negative = random.randint(52, 90)
        results.append({
            "game_id": game_id,
            "game_name": name,
            "positive": int(positive),
            "negative": int(negative),
            "alert": bool(negative > 50),
            "window_start": float(now - 30),
            "window_end": float(now),
        })
    return results


def _mock_alerts() -> list[dict]:
    return [
        {
            "game_id": "1091500",
            "game_name": "Cyberpunk 2077",
            "negative": 87,
            "positive": 12,
            "alert": True,
            "window_start": time.time() - 60,
            "window_end": time.time() - 30,
        }
    ]
