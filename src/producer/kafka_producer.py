"""
Apache Kafka Producer for Real-Time Stock Market Data
Collects live quotes from Google Finance every 5 seconds, serializes to JSON,
and publishes to the Kafka topic 'stock_ticks' with partition affinity per symbol:
- Partition 0 -> ICICIBANK
- Partition 1 -> SBIN
- Partition 2 -> HDFCBANK
- Partition 3 -> INFY
- Partition 4 -> TCS
"""

import sys
import os
import json
import time
import signal
import logging
from typing import Optional

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    COLLECTION_INTERVAL_SECONDS,
    SYMBOL_PARTITION_MAP,
    TARGET_SYMBOLS,
    EXCHANGE
)
from src.collector.google_finance import GoogleFinanceCollector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (KafkaProducer) %(message)s")
logger = logging.getLogger("KafkaStockProducer")


class StockKafkaProducer:
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.running = True
        self.collector = GoogleFinanceCollector(exchange=EXCHANGE, simulate_off_market=True)
        self.producer = None
        self._init_producer()

    def _init_producer(self):
        """Initializes Kafka Producer client with fallback for standalone environments."""
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Could not connect to Kafka ({e}). Running in resilient logging/buffer mode.")
            self.producer = None

    def publish_tick(self, quote: dict):
        """Publishes a single stock tick to its designated Kafka partition."""
        symbol = quote.get("symbol")
        partition = SYMBOL_PARTITION_MAP.get(symbol, 0)

        if self.producer:
            try:
                future = self.producer.send(
                    self.topic,
                    key=symbol,
                    value=quote,
                    partition=partition
                )
                record_metadata = future.get(timeout=5)
                logger.info(
                    f"Published {symbol} [₹{quote['price']}] to "
                    f"Topic: {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}"
                )
            except Exception as e:
                logger.error(f"Failed to publish tick for {symbol}: {e}")
        else:
            logger.info(
                f"[MOCK/BUFFER] Symbol: {symbol:<10} | Partition: {partition} | "
                f"Price: ₹{quote['price']:<8} | High: ₹{quote['high']:<8} | Low: ₹{quote['low']:<8} | Vol: {quote['volume']}"
            )

    def run(self):
        """Main loop: Fetches market data every 5 seconds and publishes to Kafka."""
        logger.info(f"Starting Stock Market Producer. Scraping Google Finance every {COLLECTION_INTERVAL_SECONDS}s...")
        logger.info(f"Target Stocks: {TARGET_SYMBOLS}")
        logger.info(f"Partition Routing: {SYMBOL_PARTITION_MAP}")

        while self.running:
            start_time = time.time()

            for symbol in TARGET_SYMBOLS:
                if not self.running:
                    break
                quote = self.collector.fetch_quote(symbol)
                if quote:
                    self.publish_tick(quote)

            elapsed = time.time() - start_time
            sleep_time = max(0.0, COLLECTION_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_time)

    def stop(self):
        """Stops the producer gracefully."""
        logger.info("Stopping Kafka Producer...")
        self.running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("Kafka Producer stopped successfully.")


if __name__ == "__main__":
    producer = StockKafkaProducer()

    def handle_sigint(sig, frame):
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    producer.run()
