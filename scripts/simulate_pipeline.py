"""
End-to-End Simulation & Verification Script
Simulates the complete real-time pipeline:
1. Google Finance Live Scraper
2. Kafka Producer Partitioning
3. Stateful Stream Processing (SMA 1m/5m, EMA, RSI, High/Low)
4. Real-time Random Forest ML Inference (BUY / SELL / HOLD)
5. Structured Event Output
"""

import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.collector.google_finance import GoogleFinanceCollector
from src.streaming.stream_engine import StreamEngine
from src.config import SYMBOL_PARTITION_MAP, TARGET_SYMBOLS


def run_pipeline_simulation(iterations: int = 3):
    print("=" * 80)
    print("🚀 STARTING REAL-TIME STOCK MARKET ANALYTICS SIMULATION")
    print(f"Target Stocks: {TARGET_SYMBOLS}")
    print(f"Partition Routing: {SYMBOL_PARTITION_MAP}")
    print("=" * 80)

    collector = GoogleFinanceCollector(exchange="NSE", simulate_off_market=True)
    engine = StreamEngine()

    for i in range(1, iterations + 1):
        print(f"\n--- [ITERATION {i}/{iterations}] Scraping Google Finance & Streaming... ---")
        start_t = time.time()

        for symbol in TARGET_SYMBOLS:
            quote = collector.fetch_quote(symbol)
            if quote:
                partition = SYMBOL_PARTITION_MAP.get(symbol, 0)
                state = engine.states[symbol]
                features = state.update(quote["price"], quote["volume"])
                prediction = engine.predict_signal(features)

                sig_color = {
                    "BUY": "\033[92mBUY\033[0m",
                    "SELL": "\033[91mSELL\033[0m",
                    "HOLD": "\033[93mHOLD\033[0m"
                }.get(prediction["signal"], prediction["signal"])

                print(
                    f"[{quote['timestamp'][:19]}] "
                    f"Symbol: {symbol:<10} | Part: {partition} | "
                    f"Price: ₹{quote['price']:<8.2f} | "
                    f"SMA-1m: ₹{features['sma_1m']:<8.2f} | "
                    f"RSI: {features['rsi_14']:<5.1f} | "
                    f"Signal: {sig_color} ({prediction['confidence']:>4.1f}%)"
                )

        elapsed = time.time() - start_t
        print(f"Iteration completed in {elapsed:.2f}s.")
        if i < iterations:
            time.sleep(2)

    print("\n" + "=" * 80)
    print("✅ Real-Time Analytics Pipeline Simulation Completed Successfully!")
    print("=" * 80)


if __name__ == "__main__":
    run_pipeline_simulation(iterations=3)
