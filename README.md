# Real-Time Stock Market Analytics System
### Apache Kafka + Apache Flink + PostgreSQL + Random Forest ML + Power BI

An end-to-end, enterprise-grade streaming analytics pipeline that collects live stock market data every 5 seconds from Google Finance, publishes partitioned events to Apache Kafka, executes stateful stream calculations and technical feature engineering in Apache Flink, performs real-time Machine Learning inference (BUY / SELL / HOLD signals) with Random Forest, persists data to PostgreSQL, and visualizes live insights in Power BI and an interactive Web Dashboard.

---

## 🏗️ Architecture Overview

```
 [1. Google Finance]
    (Live Quotes every 5s: INFY, TCS, HDFCBANK, ICICIBANK, SBIN)
            │
            ▼
 [2. Python Producer]
    (Scrapes DOM, serializes JSON, partitions by symbol)
            │
            ▼
 [3. Apache Kafka (Message Broker)]
    Topic: stock_ticks (5 Partitions)
    ├─ Partition 0 ──► ICICIBANK
    ├─ Partition 1 ──► SBIN
    ├─ Partition 2 ──► HDFCBANK
    ├─ Partition 3 ──► INFY
    └─ Partition 4 ──► TCS
            │
            ▼
 [4. Apache Flink / Stream Processing Engine]
    (Stateful Stream Processing, Keyed by Symbol, Sliding Event-Time Windows)
    ├─ Technical Indicators: SMA (1m, 5m), EMA (1m, 5m), RSI (14)
    ├─ Rolling High/Low, Volatility, Price Momentum
    └─ Real-Time Inference: Random Forest Model
            │
            ▼
 [5. Random Forest ML Model]
    Predicts: BUY / SELL / HOLD + Confidence % & Probabilities
            │
            ▼
 [6. PostgreSQL Database]
    Tables: stock_ticks, stock_features, stock_signals, daily_summary
    View: v_live_stock_dashboard
            │
            ▼
 [7. Power BI / Web Dashboard]
    (DirectQuery Mode / 5s Auto Page Refresh, Line Charts, KPI Cards, Signal Badges)
```

---

## 📁 Repository Structure

```
Stock Market Analytics/
├── docker-compose.yml              # Multi-container orchestration (Kafka, Flink, Postgres, Kafka UI)
├── requirements.txt                # Python project dependencies
├── .env                            # Environment variables & configuration
├── database/
│   └── init.sql                    # PostgreSQL schema, indexes, view & triggers
├── models/
│   ├── stock_rf_model.joblib       # Serialized Random Forest model artifact
│   └── model_metadata.json         # Feature definitions, accuracy, importance
├── docs/
│   └── power_bi_integration_guide.md # DirectQuery connection and DAX formulas
├── src/
│   ├── config.py                   # Centralized configuration & partition mapping
│   ├── collector/
│   │   └── google_finance.py       # Resilient Google Finance live quote scraper
│   ├── producer/
│   │   └── kafka_producer.py       # 5-second interval Kafka publisher with partition affinity
│   ├── streaming/
│   │   ├── stream_engine.py        # Stateful sliding window engine & RF inference
│   │   └── flink_job.py            # PyFlink streaming DataStream job
│   ├── ml/
│   │   └── train_model.py          # Offline training pipeline on technical indicators
│   └── dashboard/
│       ├── app.py                  # FastAPI real-time dashboard server
│       └── templates/
│           └── index.html          # Glassmorphism real-time analytics UI
└── scripts/
    ├── start.sh                    # Complete application startup script
    ├── stop.sh                     # Graceful shutdown script
    └── simulate_pipeline.py        # End-to-end integration test & verification
```

---

## ⚡ Quick Start

### 1. Set Up Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Verify Pipeline Simulation (Stand-alone)
Run a verified 3-iteration real-time scrape, technical indicator calculation, and ML inference test:
```bash
.venv/bin/python scripts/simulate_pipeline.py
```

### 3. Start Infrastructure with Docker
```bash
docker compose up -d
```
Services exposed:
* **Kafka Broker**: `localhost:9092`
* **Kafka UI**: [http://localhost:8085](http://localhost:8085)
* **Flink JobManager**: [http://localhost:8081](http://localhost:8081)
* **PostgreSQL**: `localhost:5432` (`stock_analytics`)

### 4. Run the Pipeline Components
In separate terminal tabs:
```bash
# Terminal 1: Ingestion Producer (fetches Google Finance every 5s)
.venv/bin/python src/producer/kafka_producer.py

# Terminal 2: Stream Processing & ML Inference Engine
.venv/bin/python src/streaming/stream_engine.py

# Terminal 3: Real-Time Web Dashboard (replicates Power BI layout)
.venv/bin/python src/dashboard/app.py
```
Open **[http://localhost:8000](http://localhost:8000)** to view live price charts, rolling moving averages, and ML trading signals!

---

## 📊 Power BI Connection (DirectQuery Mode)
1. Open **Power BI Desktop** and select **Get Data > PostgreSQL database**.
2. Enter Server: `localhost:5432`, Database: `stock_analytics`, Mode: **DirectQuery**.
3. Credentials: User `stock_user`, Password `stock_password`.
4. Select table `v_live_stock_dashboard` or tables `stock_ticks`, `stock_features`, `stock_signals`.
5. Turn on **Page Refresh** > **Auto page refresh** every **5 seconds**.
6. Refer to [`docs/power_bi_integration_guide.md`](file:///Users/vishnukanthreddy/Documents/Projects/Stock%20Market%20Analytics/docs/power_bi_integration_guide.md) for DAX formulas and conditional formatting.
