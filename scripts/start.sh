#!/bin/bash
# ==============================================================================
# Real-Time Stock Market Analytics - Application Start Process
# Starts Docker Cluster (Kafka, Flink, Postgres, Kafka UI) & Launches Pipeline
# ==============================================================================

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$DIR"

echo "=========================================================================="
echo " Starting Real-Time Stock Market Analytics System"
echo "=========================================================================="

# 1. Check Python virtual environment
if [ ! -d ".venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv .venv
    .venv/bin/pip install --upgrade pip
    .venv/bin/pip install -r requirements.txt
fi

# 2. Check and Train Machine Learning Model if not present
if [ ! -f "models/stock_rf_model.joblib" ]; then
    echo "Training initial Random Forest ML model on historical indicators..."
    .venv/bin/python src/ml/train_model.py
else
    echo "Random Forest model artifact verified at models/stock_rf_model.joblib"
fi

# 3. Check Docker status
if docker info >/dev/null 2>&1; then
    echo "Docker daemon is running. Starting infrastructure services via Docker Compose..."
    docker compose up -d zookeeper kafka kafka-init-topics postgres flink-jobmanager flink-taskmanager kafka-ui
    echo "Waiting 10 seconds for services to initialize..."
    sleep 10
else
    echo "NOTE: Docker daemon is not active. Running in standalone local buffer mode."
    echo "      (Start Docker Desktop to enable full containerized Kafka, Flink, and PostgreSQL)."
fi

echo ""
echo "=========================================================================="
echo " Infrastructure Endpoints:"
echo "   - Web Dashboard:     http://localhost:8000"
echo "   - Kafka UI:          http://localhost:8085"
echo "   - Flink Dashboard:   http://localhost:8081"
echo "   - PostgreSQL:        localhost:5432 (DB: stock_analytics, User: stock_user)"
echo "=========================================================================="
echo ""

echo "To launch the Web Dashboard:        .venv/bin/python src/dashboard/app.py"
echo "To launch the Google Finance Scraper Producer: .venv/bin/python src/producer/kafka_producer.py"
echo "To launch the Stream Processing Engine:        .venv/bin/python src/streaming/stream_engine.py"
