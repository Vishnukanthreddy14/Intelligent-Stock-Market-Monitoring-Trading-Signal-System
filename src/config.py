import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_ticks")
KAFKA_NUM_PARTITIONS = int(os.getenv("KAFKA_NUM_PARTITIONS", "5"))
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "stock_analytics_group")

# PostgreSQL Settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "stock_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "stock_password")

# Data Collection Settings
COLLECTION_INTERVAL_SECONDS = int(os.getenv("COLLECTION_INTERVAL_SECONDS", "5"))
EXCHANGE = os.getenv("EXCHANGE", "NSE")

# Explicit Partition Mapping matching the Architecture Diagram:
# Partition 0 -> ICICIBANK
# Partition 1 -> SBIN
# Partition 2 -> HDFCBANK
# Partition 3 -> INFY
# Partition 4 -> TCS
SYMBOL_PARTITION_MAP = {
    "ICICIBANK": 0,
    "SBIN": 1,
    "HDFCBANK": 2,
    "INFY": 3,
    "TCS": 4
}

TARGET_SYMBOLS = list(SYMBOL_PARTITION_MAP.keys())
