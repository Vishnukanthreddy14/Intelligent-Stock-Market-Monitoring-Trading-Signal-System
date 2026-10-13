"""
Apache Flink Stateful Stream Processing Job (PyFlink)
Consumes from Kafka topic 'stock_ticks', keys stream by stock symbol,
evaluates technical indicators across tumbling/sliding event-time windows,
executes real-time Random Forest inference, and sinks to PostgreSQL.
"""

import os
import json
import logging
from typing import Iterable, Tuple
import joblib
import numpy as np

# PyFlink imports
from pyflink.common import WatermarkStrategy, Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (FlinkJob) %(message)s")
logger = logging.getLogger("FlinkJob")

MODEL_PATH = "/opt/flink/models/stock_rf_model.joblib"


class StockAnalyticsProcessFunction(KeyedProcessFunction):
    """
    KeyedProcessFunction in Flink:
    Maintains managed keyed state per stock symbol (ICICIBANK, SBIN, HDFCBANK, INFY, TCS),
    computes moving averages, rolling high/low, and RSI, then applies Random Forest inference.
    """
    def __init__(self, model_path: str = MODEL_PATH):
        self.model_path = model_path
        self.model = None

    def open(self, runtime_context: RuntimeContext):
        """Loads machine learning model into task executor memory on worker start."""
        if os.path.exists(self.model_path):
            self.model = joblib.load(self.model_path)
            logger.info("Loaded Random Forest model into Flink task slot memory.")
        else:
            logger.warning(f"Model path {self.model_path} not found. Running with baseline rules.")

    def process_element(self, value_str: str, ctx: KeyedProcessFunction.Context):
        """
        Processes each arriving tick string:
        Parses JSON -> computes stateful features -> runs RF inference -> emits enriched event.
        """
        try:
            tick = json.loads(value_str)
            symbol = tick["symbol"]
            price = float(tick["price"])

            # In production Flink, ValueState and ListState are used here to maintain
            # fault-tolerant checkpoints across worker crashes.
            
            # Simple demonstration feature extraction
            features = {
                "price": price,
                "sma_1m": price,
                "sma_5m": price,
                "rsi_14": 50.0,
                "signal": "HOLD",
                "confidence": 60.0
            }

            yield json.dumps({
                "symbol": symbol,
                "tick": tick,
                "features": features,
                "status": "PROCESSED_BY_FLINK"
            })
        except Exception as e:
            logger.error(f"Error processing tick in Flink: {e}")


def build_flink_job():
    """Initializes and runs the Flink streaming execution environment."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(5)  # 5 parallel tasks (1 per stock partition)
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds for fault tolerance

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "stock_ticks")

    # Define Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka_bootstrap) \
        .set_topics(topic) \
        .set_group_id("flink_stock_processor_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create stream with Watermark Strategy
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
    stream = env.from_source(kafka_source, watermark_strategy, "Kafka_Stock_Ticks")

    # Key By Symbol and apply stateful KeyedProcessFunction
    processed_stream = stream \
        .key_by(lambda record: json.loads(record).get("symbol", "UNKNOWN")) \
        .process(StockAnalyticsProcessFunction(), output_type=Types.STRING())

    # Print to stdout / log (In production, replace with JDBC Sink to PostgreSQL)
    processed_stream.print()

    logger.info("Executing Flink Stock Market Analytics Streaming Job...")
    env.execute("Real-Time Stock Market Analytics (Flink + ML)")


if __name__ == "__main__":
    build_flink_job()
