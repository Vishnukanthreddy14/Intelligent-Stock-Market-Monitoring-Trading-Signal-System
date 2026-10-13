"""
Stateful Real-Time Stream Processing Engine
Processes live stock ticks from Kafka (or direct stream), maintains keyed sliding
windows per symbol, computes technical indicators (SMA, EMA, RSI, Rolling High/Low),
executes real-time Random Forest inference, and persists results to PostgreSQL.
"""

import os
import sys
import json
import time
import math
import logging
import collections
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import joblib
import numpy as np

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    TARGET_SYMBOLS
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (StreamEngine) %(message)s")
logger = logging.getLogger("StreamProcessor")

MODEL_PATH = os.path.join(os.path.dirname(__file__), "../../models/stock_rf_model.joblib")
META_PATH = os.path.join(os.path.dirname(__file__), "../../models/model_metadata.json")


class SymbolStreamState:
    """
    Stateful rolling window accumulator for a single stock symbol.
    Maintains tick history for 1m (12 ticks), 5m (60 ticks), and RSI calculations.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.prices_1m = collections.deque(maxlen=12)    # 1-minute window (12 ticks @ 5s)
        self.prices_5m = collections.deque(maxlen=60)    # 5-minute window (60 ticks @ 5s)
        self.gains_14 = collections.deque(maxlen=14)     # RSI 14 gains
        self.losses_14 = collections.deque(maxlen=14)    # RSI 14 losses
        self.last_price: Optional[float] = None
        self.ema_1m: Optional[float] = None
        self.ema_5m: Optional[float] = None

    def update(self, price: float, volume: int) -> Dict[str, Any]:
        """Updates internal state with a new tick and calculates technical indicators."""
        # 1. Update rolling price deques
        self.prices_1m.append(price)
        self.prices_5m.append(price)

        # 2. Moving Averages
        sma_1m = sum(self.prices_1m) / len(self.prices_1m)
        sma_5m = sum(self.prices_5m) / len(self.prices_5m)

        # 3. Exponential Moving Averages (EMA)
        k_1m = 2.0 / (12 + 1)
        k_5m = 2.0 / (60 + 1)
        if self.ema_1m is None:
            self.ema_1m = price
            self.ema_5m = price
        else:
            self.ema_1m = (price * k_1m) + (self.ema_1m * (1 - k_1m))
            self.ema_5m = (price * k_5m) + (self.ema_5m * (1 - k_5m))

        # 4. Rolling High & Low (5m window)
        rolling_high = max(self.prices_5m)
        rolling_low = min(self.prices_5m)

        # 5. RSI (14 periods)
        if self.last_price is not None:
            delta = price - self.last_price
            gain = max(delta, 0.0)
            loss = max(-delta, 0.0)
            self.gains_14.append(gain)
            self.losses_14.append(loss)

        if len(self.gains_14) > 0 and sum(self.losses_14) > 0:
            avg_gain = sum(self.gains_14) / len(self.gains_14)
            avg_loss = sum(self.losses_14) / len(self.losses_14)
            rs = avg_gain / (avg_loss + 1e-9)
            rsi_14 = 100.0 - (100.0 / (1.0 + rs))
        else:
            rsi_14 = 50.0  # Neutral baseline until enough ticks

        # 6. Returns, Momentum, and Volatility
        returns_pct = ((price - self.last_price) / self.last_price) if self.last_price else 0.0
        
        # Momentum: price change over the last 6 ticks (~30s)
        if len(self.prices_1m) >= 6:
            price_momentum = (price - self.prices_1m[-6]) / self.prices_1m[-6]
        else:
            price_momentum = 0.0

        # Volatility: standard deviation of returns in 1m window
        if len(self.prices_1m) > 2:
            pts = list(self.prices_1m)
            rets = [(pts[i] - pts[i - 1]) / pts[i - 1] for i in range(1, len(pts))]
            volatility = float(np.std(rets))
        else:
            volatility = 0.0005

        self.last_price = price

        return {
            "sma_1m": round(sma_1m, 2),
            "sma_5m": round(sma_5m, 2),
            "ema_1m": round(self.ema_1m, 2),
            "ema_5m": round(self.ema_5m, 2),
            "rolling_high": round(rolling_high, 2),
            "rolling_low": round(rolling_low, 2),
            "rsi_14": round(rsi_14, 2),
            "price_momentum": round(price_momentum, 6),
            "returns_pct": round(returns_pct, 6),
            "volatility": round(volatility, 6),
            # Model normalized feature vectors:
            "price_to_sma1m": (price - sma_1m) / sma_1m,
            "price_to_sma5m": (price - sma_5m) / sma_5m,
            "sma_cross": (sma_1m - sma_5m) / sma_5m,
            "rolling_high_diff": (rolling_high - price) / price,
            "rolling_low_diff": (price - rolling_low) / price
        }


class StreamEngine:
    """
    Main Stream Processing coordinator.
    Reads ticks from Kafka or ingestion pipeline, applies stateful transformations,
    evaluates Random Forest model, and writes to PostgreSQL.
    """
    def __init__(self):
        self.states: Dict[str, SymbolStreamState] = {sym: SymbolStreamState(sym) for sym in TARGET_SYMBOLS}
        self.model = None
        self.model_classes = []
        self._load_ml_model()
        self.db_conn = None
        self._connect_db()

    def _load_ml_model(self):
        """Loads serialized Random Forest model from disk."""
        try:
            if os.path.exists(MODEL_PATH):
                self.model = joblib.load(MODEL_PATH)
                logger.info(f"Loaded Random Forest model from {MODEL_PATH}")
                if os.path.exists(META_PATH):
                    with open(META_PATH, "r") as f:
                        meta = json.load(f)
                        self.model_classes = meta.get("classes", ["BUY", "HOLD", "SELL"])
                else:
                    self.model_classes = ["BUY", "HOLD", "SELL"]
            else:
                logger.warning(f"Model file not found at {MODEL_PATH}. Inference will use heuristic logic.")
        except Exception as e:
            logger.error(f"Error loading ML model: {e}")

    def _connect_db(self):
        """Establishes connection to PostgreSQL database."""
        try:
            import psycopg2
            self.db_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=3
            )
            self.db_conn.autocommit = True
            logger.info(f"Connected to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        except Exception as e:
            logger.warning(f"Could not connect to PostgreSQL ({e}). Operating in memory/logging mode.")
            self.db_conn = None

    def predict_signal(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes real-time inference on the calculated technical features.
        Returns predicted signal (BUY/SELL/HOLD), confidence %, and probabilities.
        """
        if self.model:
            import pandas as pd
            # Build feature DataFrame matching training schema
            feat_df = pd.DataFrame([{
                "price_to_sma1m": features["price_to_sma1m"],
                "price_to_sma5m": features["price_to_sma5m"],
                "sma_cross": features["sma_cross"],
                "rsi_14": features["rsi_14"],
                "rolling_high_diff": features["rolling_high_diff"],
                "rolling_low_diff": features["rolling_low_diff"],
                "returns_pct": features["returns_pct"],
                "price_momentum": features["price_momentum"],
                "volatility": features["volatility"]
            }])

            probs = self.model.predict_proba(feat_df)[0]
            pred_idx = np.argmax(probs)
            pred_signal = self.model.classes_[pred_idx]
            confidence = float(probs[pred_idx] * 100.0)

            # Map probabilities
            class_to_prob = dict(zip(self.model.classes_, probs))
            prob_buy = float(class_to_prob.get("BUY", 0.0))
            prob_sell = float(class_to_prob.get("SELL", 0.0))
            prob_hold = float(class_to_prob.get("HOLD", 0.0))

            return {
                "signal": pred_signal,
                "confidence": round(confidence, 2),
                "prob_buy": round(prob_buy, 4),
                "prob_sell": round(prob_sell, 4),
                "prob_hold": round(prob_hold, 4)
            }
        else:
            # Resilient rule-based heuristic when model artifact is unavailable
            rsi = features["rsi_14"]
            sma_cross = features["sma_cross"]
            if rsi < 35 and sma_cross > 0:
                signal, conf = "BUY", 75.0
            elif rsi > 65 and sma_cross < 0:
                signal, conf = "SELL", 75.0
            else:
                signal, conf = "HOLD", 60.0

            return {
                "signal": signal,
                "confidence": conf,
                "prob_buy": 0.75 if signal == "BUY" else 0.15,
                "prob_sell": 0.75 if signal == "SELL" else 0.15,
                "prob_hold": 0.60 if signal == "HOLD" else 0.20
            }

    def process_tick(self, tick: Dict[str, Any]):
        """
        Processes a single stock tick through the end-to-end pipeline:
        1. State update & Feature calculation
        2. Random Forest inference
        3. Database persistence
        """
        symbol = tick["symbol"]
        price = float(tick["price"])
        volume = int(tick.get("volume", 0))
        timestamp = tick.get("timestamp") or datetime.now(timezone.utc).isoformat()

        # Step 1: Update state and compute features
        state = self.states.setdefault(symbol, SymbolStreamState(symbol))
        features = state.update(price, volume)

        # Step 2: ML Inference
        prediction = self.predict_signal(features)

        # Step 3: Log output
        logger.info(
            f"⚡ [STREAM] {symbol:<10} | Price: ₹{price:<8.2f} | "
            f"SMA(1m): ₹{features['sma_1m']:<8.2f} | SMA(5m): ₹{features['sma_5m']:<8.2f} | "
            f"RSI: {features['rsi_14']:<5.1f} | SIGNAL: {prediction['signal']:<5} "
            f"({prediction['confidence']:.1f}% conf)"
        )

        # Step 4: Persist to PostgreSQL tables
        self._persist_to_postgres(tick, features, prediction, timestamp)

    def _persist_to_postgres(self, tick: dict, features: dict, prediction: dict, timestamp: str):
        """Writes raw tick, features, and signals into PostgreSQL."""
        if not self.db_conn:
            return

        try:
            with self.db_conn.cursor() as cur:
                # 1. Insert into stock_ticks
                cur.execute(
                    """
                    INSERT INTO stock_ticks (symbol, price, open_price, high_price, low_price, previous_close, volume, timestamp, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        tick["symbol"],
                        tick["price"],
                        tick.get("open", tick["price"]),
                        tick.get("high", tick["price"]),
                        tick.get("low", tick["price"]),
                        tick.get("previous_close", tick["price"]),
                        tick.get("volume", 0),
                        timestamp,
                        tick.get("source", "stream_engine")
                    )
                )

                # 2. Insert into stock_features
                cur.execute(
                    """
                    INSERT INTO stock_features (symbol, timestamp, price, sma_1m, sma_5m, ema_1m, ema_5m,
                                               rolling_high, rolling_low, rsi_14, price_momentum, returns_pct, volatility)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        tick["symbol"],
                        timestamp,
                        tick["price"],
                        features["sma_1m"],
                        features["sma_5m"],
                        features["ema_1m"],
                        features["ema_5m"],
                        features["rolling_high"],
                        features["rolling_low"],
                        features["rsi_14"],
                        features["price_momentum"],
                        features["returns_pct"],
                        features["volatility"]
                    )
                )

                # 3. Insert into stock_signals
                cur.execute(
                    """
                    INSERT INTO stock_signals (symbol, timestamp, price, signal, confidence, prob_buy, prob_sell, prob_hold, features_used)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        tick["symbol"],
                        timestamp,
                        tick["price"],
                        prediction["signal"],
                        prediction["confidence"],
                        prediction["prob_buy"],
                        prediction["prob_sell"],
                        prediction["prob_hold"],
                        json.dumps({
                            "sma_1m": features["sma_1m"],
                            "sma_5m": features["sma_5m"],
                            "rsi_14": features["rsi_14"],
                            "volatility": features["volatility"]
                        })
                    )
                )
        except Exception as e:
            logger.error(f"Error persisting stream event to PostgreSQL: {e}")
            # Reset connection if corrupted
            self._connect_db()

    def run_kafka_consumer(self):
        """Subscribes to Kafka topic and processes incoming stream continuously."""
        logger.info(f"Subscribing to Kafka topic '{KAFKA_TOPIC}'...")
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            logger.info("Kafka consumer active. Awaiting stock ticks...")
            for message in consumer:
                tick = message.value
                self.process_tick(tick)
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")


if __name__ == "__main__":
    engine = StreamEngine()
    engine.run_kafka_consumer()
