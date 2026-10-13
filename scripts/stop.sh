#!/bin/bash
# ==============================================================================
# Real-Time Stock Market Analytics - Graceful Application Stop Process
# ==============================================================================

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$DIR"

echo "Stopping Python producers, stream engines, and dashboard..."
pkill -f "src/producer/kafka_producer.py" || true
pkill -f "src/streaming/stream_engine.py" || true
pkill -f "src/dashboard/app.py" || true

if docker info >/dev/null 2>&1; then
    echo "Stopping Docker containers..."
    docker compose down
fi

echo "Pipeline shut down gracefully."
