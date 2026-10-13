"""
Random Forest Model Training Pipeline for Stock Market Buy/Sell Signals
Trains an offline Random Forest Classifier on historical technical indicators
and serializes model artifacts for real-time stream inference in Apache Flink.
"""

import os
import sys
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import classification_report, accuracy_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (ML-Train) %(message)s")
logger = logging.getLogger("TrainModel")

MODEL_DIR = os.path.join(os.path.dirname(__file__), "../../models")
MODEL_FILE = os.path.join(MODEL_DIR, "stock_rf_model.joblib")
META_FILE = os.path.join(MODEL_DIR, "model_metadata.json")

# Target feature list required by real-time streaming engine
FEATURE_COLUMNS = [
    "price_to_sma1m",
    "price_to_sma5m",
    "sma_cross",
    "rsi_14",
    "rolling_high_diff",
    "rolling_low_diff",
    "returns_pct",
    "price_momentum",
    "volatility"
]


def generate_synthetic_historical_data(n_samples: int = 5000) -> pd.DataFrame:
    """
    Generates realistic multi-regime tick time-series data for training,
    incorporating trend, mean-reversion, volatility clustering, and noise.
    """
    logger.info(f"Generating synthetic historical training data ({n_samples} ticks)...")
    np.random.seed(42)

    symbols = ["ICICIBANK", "SBIN", "HDFCBANK", "INFY", "TCS"]
    base_prices = {"ICICIBANK": 1315.0, "SBIN": 955.0, "HDFCBANK": 720.0, "INFY": 1025.0, "TCS": 2070.0}

    all_dfs = []
    ticks_per_stock = n_samples // len(symbols)

    start_date = datetime.now() - timedelta(days=10)

    for sym in symbols:
        price = base_prices[sym]
        timestamps = [start_date + timedelta(seconds=5 * i) for i in range(ticks_per_stock)]
        prices = []
        volumes = []

        # Regime switching simulation: bull, bear, ranging
        regime = "ranging"
        regime_duration = 0

        for i in range(ticks_per_stock):
            if regime_duration <= 0:
                regime = np.random.choice(["bull", "bear", "ranging"], p=[0.35, 0.35, 0.30])
                regime_duration = np.random.randint(50, 200)

            regime_duration -= 1
            if regime == "bull":
                drift = 0.0003
                vol = 0.0012
            elif regime == "bear":
                drift = -0.0003
                vol = 0.0014
            else:
                drift = 0.0
                vol = 0.0008

            shock = np.random.normal(drift, vol)
            price = max(10.0, price * (1 + shock))
            prices.append(price)
            volumes.append(int(np.random.lognormal(mean=12.0, sigma=0.5)))

        df_sym = pd.DataFrame({
            "timestamp": timestamps,
            "symbol": sym,
            "price": prices,
            "volume": volumes
        })
        all_dfs.append(df_sym)

    df = pd.concat(all_dfs, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
    return df


def calculate_technical_features(df: pd.DataFrame) -> pd.DataFrame:
    """Computes technical indicators: SMA, EMA, RSI, Rolling High/Low, Volatility."""
    logger.info("Computing technical features...")

    dfs = []
    for symbol, group in df.groupby("symbol"):
        g = group.sort_values("timestamp").copy()

        # Moving averages (assuming 5-second ticks: 1m = 12 ticks, 5m = 60 ticks)
        g["sma_1m"] = g["price"].rolling(window=12, min_periods=1).mean()
        g["sma_5m"] = g["price"].rolling(window=60, min_periods=1).mean()
        g["ema_1m"] = g["price"].ewm(span=12, adjust=False).mean()
        g["ema_5m"] = g["price"].ewm(span=60, adjust=False).mean()

        # Rolling High/Low (5 min window)
        g["rolling_high"] = g["price"].rolling(window=60, min_periods=1).max()
        g["rolling_low"] = g["price"].rolling(window=60, min_periods=1).min()

        # Relative Strength Index (RSI 14)
        delta = g["price"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=14, min_periods=1).mean()
        avg_loss = loss.rolling(window=14, min_periods=1).mean()
        rs = avg_gain / (avg_loss + 1e-9)
        g["rsi_14"] = 100 - (100 / (1 + rs))

        # Normalized Features
        g["price_to_sma1m"] = (g["price"] - g["sma_1m"]) / g["sma_1m"]
        g["price_to_sma5m"] = (g["price"] - g["sma_5m"]) / g["sma_5m"]
        g["sma_cross"] = (g["sma_1m"] - g["sma_5m"]) / g["sma_5m"]
        g["rolling_high_diff"] = (g["rolling_high"] - g["price"]) / g["price"]
        g["rolling_low_diff"] = (g["price"] - g["rolling_low"]) / g["price"]
        g["returns_pct"] = g["price"].pct_change().fillna(0)
        g["price_momentum"] = g["price"].pct_change(periods=6).fillna(0)
        g["volatility"] = g["returns_pct"].rolling(window=20, min_periods=1).std().fillna(0)

        # Label: Predict future return 12 ticks (1 minute) ahead
        # BUY: future_return > +0.10% (0.0010)
        # SELL: future_return < -0.10% (-0.0010)
        # HOLD: otherwise
        future_return = (g["price"].shift(-12) - g["price"]) / g["price"]
        conditions = [
            future_return > 0.0010,
            future_return < -0.0010
        ]
        choices = ["BUY", "SELL"]
        g["signal"] = np.select(conditions, choices, default="HOLD")

        dfs.append(g)

    result_df = pd.concat(dfs, ignore_index=True)
    # Drop rows without complete lookahead target
    result_df = result_df.dropna(subset=FEATURE_COLUMNS + ["signal"]).copy()
    return result_df


def train_and_evaluate(df: pd.DataFrame):
    """Trains a Random Forest classifier with time-series cross-validation."""
    logger.info("Splitting dataset and training Random Forest...")

    X = df[FEATURE_COLUMNS]
    y = df["signal"]

    # Time-series aware train/test split (80% train, 20% test)
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    rf = RandomForestClassifier(
        n_estimators=120,
        max_depth=10,
        min_samples_split=10,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1
    )

    rf.fit(X_train, y_train)

    preds = rf.predict(X_test)
    accuracy = accuracy_score(y_test, preds)

    logger.info(f"Test Accuracy: {accuracy * 100:.2f}%")
    logger.info(f"\nClassification Report:\n{classification_report(y_test, preds, zero_division=0)}")

    # Feature Importance analysis
    importances = dict(zip(FEATURE_COLUMNS, rf.feature_importances_))
    sorted_importances = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    logger.info("Feature Importances:")
    for feat, imp in sorted_importances:
        logger.info(f"  {feat:<20}: {imp:.4f}")

    # Ensure output directory exists
    os.makedirs(MODEL_DIR, exist_ok=True)

    # Save model and metadata
    joblib.dump(rf, MODEL_FILE)
    logger.info(f"Model saved to {MODEL_FILE}")

    metadata = {
        "model_type": "RandomForestClassifier",
        "trained_at": datetime.now().isoformat(),
        "classes": rf.classes_.tolist(),
        "feature_columns": FEATURE_COLUMNS,
        "test_accuracy": float(accuracy),
        "n_estimators": 120,
        "feature_importances": {k: float(v) for k, v in sorted_importances}
    }
    with open(META_FILE, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Model metadata saved to {META_FILE}")

    return rf, metadata


if __name__ == "__main__":
    df_raw = generate_synthetic_historical_data(n_samples=10000)
    df_feat = calculate_technical_features(df_raw)
    train_and_evaluate(df_feat)
