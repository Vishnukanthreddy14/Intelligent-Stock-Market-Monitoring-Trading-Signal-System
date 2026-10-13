"""
FastAPI Real-Time Stock Market Dashboard Server
Provides high-performance REST APIs and real-time streaming endpoints
for visualizing live stock ticks, moving averages, and Random Forest signals.
"""

import os
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    TARGET_SYMBOLS
)
from src.collector.google_finance import GoogleFinanceCollector
from src.streaming.stream_engine import SymbolStreamState, StreamEngine

app = FastAPI(title="Real-Time Stock Market Analytics Dashboard")

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# In-memory streaming fallback buffer
collector = GoogleFinanceCollector(exchange="NSE", simulate_off_market=True)
stream_engine = StreamEngine()

SYMBOL_NAMES = {
    "ICICIBANK": "ICICI Bank",
    "SBIN": "State Bank of India",
    "HDFCBANK": "HDFC Bank",
    "INFY": "Infosys",
    "TCS": "Tata Consultancy Services"
}

# In-memory ring buffer for immediate real-time rendering
tick_history: Dict[str, List[Dict[str, Any]]] = {sym: [] for sym in TARGET_SYMBOLS}
signals_log: List[Dict[str, Any]] = []


def get_db_connection():
    """Returns a connection to PostgreSQL if available."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=2
        )
        return conn
    except Exception:
        return None


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serves the main real-time dashboard UI."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/dashboard-data")
async def get_dashboard_data(symbol: str = "INFY"):
    """
    Returns aggregated real-time stock cards, active chart series,
    and recent machine learning signals.
    """
    conn = get_db_connection()
    cards = []

    # 1. Try querying PostgreSQL View first
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM v_live_stock_dashboard;")
                rows = cur.fetchall()
                for row in rows:
                    sym = row["symbol"]
                    price = float(row["current_price"])
                    open_p = float(row["open_price"] or price)
                    change_pct = ((price - open_p) / open_p * 100.0) if open_p else 0.0

                    cards.append({
                        "symbol": sym,
                        "name": SYMBOL_NAMES.get(sym, sym),
                        "price": price,
                        "high": float(row["day_high"] or price),
                        "low": float(row["day_low"] or price),
                        "change_pct": round(change_pct, 2),
                        "sma_1m": float(row["sma_1m"] or price),
                        "sma_5m": float(row["sma_5m"] or price),
                        "signal": row["current_signal"] or "HOLD",
                        "confidence": float(row["signal_confidence"] or 60.0)
                    })

                # Fetch history for selected symbol
                cur.execute(
                    """
                    SELECT t.timestamp, t.price, f.sma_1m
                    FROM stock_ticks t
                    LEFT JOIN stock_features f ON t.symbol = f.symbol AND t.timestamp = f.timestamp
                    WHERE t.symbol = %s
                    ORDER BY t.timestamp DESC
                    LIMIT 30;
                    """,
                    (symbol,)
                )
                hist_rows = cur.fetchall()
                hist_rows.reverse()
                history = [
                    {
                        "time": h["timestamp"].strftime("%H:%M:%S") if hasattr(h["timestamp"], "strftime") else str(h["timestamp"]),
                        "price": float(h["price"]),
                        "sma_1m": float(h["sma_1m"] or h["price"])
                    }
                    for h in hist_rows
                ]

                # Fetch recent signals table
                cur.execute(
                    """
                    SELECT s.timestamp, s.symbol, s.price, s.signal, s.confidence, f.sma_1m, f.rsi_14
                    FROM stock_signals s
                    LEFT JOIN stock_features f ON s.symbol = f.symbol AND s.timestamp = f.timestamp
                    ORDER BY s.timestamp DESC
                    LIMIT 15;
                    """,
                )
                sig_rows = cur.fetchall()
                recent_signals = [
                    {
                        "time": s["timestamp"].strftime("%H:%M:%S") if hasattr(s["timestamp"], "strftime") else str(s["timestamp"]),
                        "symbol": s["symbol"],
                        "price": float(s["price"]),
                        "signal": s["signal"],
                        "confidence": float(s["confidence"]),
                        "sma_1m": float(s["sma_1m"] or s["price"]),
                        "rsi_14": float(s["rsi_14"] or 50.0)
                    }
                    for s in sig_rows
                ]

                conn.close()
                return {"cards": cards, "history": history, "recent_signals": recent_signals}
        except Exception:
            if conn:
                conn.close()

    # 2. Resilient In-Memory Live Pipeline Fallback (for instant standalone demo)
    for sym in TARGET_SYMBOLS:
        quote = collector.fetch_quote(sym)
        if quote:
            state = stream_engine.states[sym]
            feat = state.update(quote["price"], quote["volume"])
            pred = stream_engine.predict_signal(feat)

            open_p = quote.get("open", quote["price"])
            change_pct = ((quote["price"] - open_p) / open_p * 100.0) if open_p else 0.0
            time_str = datetime.now().strftime("%H:%M:%S")

            cards.append({
                "symbol": sym,
                "name": SYMBOL_NAMES.get(sym, sym),
                "price": quote["price"],
                "high": quote["high"],
                "low": quote["low"],
                "change_pct": round(change_pct, 2),
                "sma_1m": feat["sma_1m"],
                "sma_5m": feat["sma_5m"],
                "signal": pred["signal"],
                "confidence": pred["confidence"]
            })

            # Record history
            tick_history[sym].append({
                "time": time_str,
                "price": quote["price"],
                "sma_1m": feat["sma_1m"]
            })
            if len(tick_history[sym]) > 40:
                tick_history[sym].pop(0)

            # Record signals log
            signals_log.insert(0, {
                "time": time_str,
                "symbol": sym,
                "price": quote["price"],
                "signal": pred["signal"],
                "confidence": pred["confidence"],
                "sma_1m": feat["sma_1m"],
                "rsi_14": feat["rsi_14"]
            })

    if len(signals_log) > 20:
        signals_log[:] = signals_log[:20]

    return {
        "cards": cards,
        "history": tick_history.get(symbol, []),
        "recent_signals": signals_log
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.dashboard.app:app", host="0.0.0.0", port=8000, reload=True)
