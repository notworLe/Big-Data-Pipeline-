"""
main.py — FastAPI Dashboard Server
Steam Analytics: Lịch sử (Hive) + Real-time (MongoDB)

Chạy:
    cd dashboard
    uvicorn main:app --reload --host 0.0.0.0 --port 8000

Mở browser: http://localhost:8000
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from hive_client import (
    get_sentiment_by_genre,
    get_top_games,
    get_trend_by_month,
    get_trend_by_year,
    invalidate_cache,
)
from mongo_client import get_alerts, get_realtime_sentiment


def _safe_json(obj) -> str:
    """JSON serializer that handles datetime, ObjectId, and other non-standard types."""
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        # bson ObjectId (if pymongo is installed)
        try:
            from bson import ObjectId
            if isinstance(o, ObjectId):
                return str(o)
        except ImportError:
            pass
        return str(o)  # fallback: convert anything to string
    return json.dumps(obj, default=default)

# ── App setup ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Steam Analytics Dashboard",
    description="Big Data Pipeline — Lịch sử (Hive/HDFS) + Real-time (MongoDB)",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── Root ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


# ── Tab 1: Lịch sử — query Hive (có cache 1 giờ) ───────────────────────────

@app.get("/api/top-games")
async def api_top_games():
    """Top 10 game theo recommend_rate (từ Hive Warehouse)."""
    return get_top_games()


@app.get("/api/sentiment-by-genre")
async def api_sentiment_by_genre():
    """Recommend rate theo genre (từ Hive Warehouse)."""
    return get_sentiment_by_genre()


@app.get("/api/trend-by-year")
async def api_trend_by_year():
    """Trend sentiment theo năm (từ Hive Warehouse)."""
    return get_trend_by_year()


@app.get("/api/trend-by-month")
async def api_trend_by_month():
    """Trend sentiment theo tháng — 24 tháng gần nhất (từ Hive Warehouse)."""
    return get_trend_by_month()


@app.post("/api/cache/clear")
async def api_clear_cache():
    """Force invalidate Hive cache — dùng khi chạy ETL batch mới."""
    invalidate_cache()
    return {"status": "ok", "message": "Cache cleared"}


# ── Tab 2: Real-time — SSE từ MongoDB ───────────────────────────────────────

@app.get("/api/stream/realtime")
async def api_realtime_stream():
    """
    SSE endpoint — push sentiment data từ MongoDB mỗi 10 giây.
    Browser dùng: const es = new EventSource('/api/stream/realtime')
    """
    async def event_generator():
        while True:
            try:
                data = get_realtime_sentiment(limit=20)
                yield f"data: {_safe_json(data)}\n\n"
            except Exception as e:
                yield f"data: {_safe_json({'error': str(e)})}\n\n"

            await asyncio.sleep(9)
            yield ": ping\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # tắt Nginx buffer, SSE chạy mượt
            "Connection": "keep-alive",
        },
    )


@app.get("/api/alerts")
async def api_alerts():
    """Danh sách game bị alert (negative > 50 trong 30s)."""
    return get_alerts(limit=10)


# ── Health check ─────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "Steam Analytics Dashboard"}
