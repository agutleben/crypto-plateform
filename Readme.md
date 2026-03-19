# 🚀 Real-Time Crypto Analytics Platform

A production-grade real-time cryptocurrency analytics platform built with **Apache Kafka**, **Spark Structured Streaming**, **Google BigQuery**, **dbt**, **Apache Airflow**, **FastAPI**, and **React**.

> Ingests live trade data from Binance WebSocket, processes it through a distributed streaming pipeline, stores it in BigQuery, and visualizes it in a real-time dashboard.

---

## 📐 Architecture

```
Binance WebSocket
       │
       ▼
   Kafka (KRaft)          ← Raw trade events (topic: crypto.raw.trades)
       │
       ▼
Spark Structured Streaming ← VWAP, volume, price metrics (1-min sliding window)
       │
       ├──────────────────► BigQuery RAW (trades, metrics)
       │
       ▼
     dbt                  ← Staging views + Mart tables (top movers, OHLCV, heatmap)
       │
       ▼
   Airflow                ← Orchestrates dbt runs every 15 min + spike alerts every 5 min
       │
       ▼
   FastAPI                ← REST + WebSocket API
       │
       ▼
  React Dashboard         ← Live Top Movers, OHLCV chart, Heatmap, Alerts
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Streaming** | Binance WebSocket, Apache Kafka 3.8 (KRaft) |
| **Processing** | Apache Spark 3.5 Structured Streaming |
| **Data Warehouse** | Google BigQuery |
| **Transformation** | dbt-bigquery 1.8 |
| **Orchestration** | Apache Airflow 2.9 + PostgreSQL |
| **Backend** | FastAPI + Uvicorn |
| **Frontend** | React 18 + TypeScript + Vite + Tailwind CSS v4 |
| **Infrastructure** | Docker Compose |
| **Deployment** | Netlify (frontend) |

---

## 📦 Project Structure

```
crypto-platform/
├── producer/               # Binance WebSocket → Kafka producer
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── spark/                  # Spark Structured Streaming job
│   ├── Dockerfile
│   └── jobs/
│       └── streaming.py
├── dbt/
│   └── crypto_platform/    # dbt project
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_trades.sql
│       │   │   └── stg_metrics.sql
│       │   └── mart/
│       │       ├── mart_top_movers.sql
│       │       ├── mart_ohlcv_1h.sql
│       │       └── mart_volume_heatmap.sql
│       └── tests/
├── airflow/                # Airflow DAGs + Dockerfile
│   ├── Dockerfile
│   ├── init.sh
│   └── dags/
│       ├── dag_dbt_refresh.py
│       └── dag_alerts.py
├── api/                    # FastAPI backend
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── secrets/                # GCP credentials (gitignored)
├── docker-compose.yml
└── .env
```

---

## 🚀 Getting Started

### Prerequisites

- Docker Desktop with WSL2 integration enabled
- Google Cloud account with BigQuery enabled
- Node.js 18+

### 1. Clone & Configure

```bash
git clone https://github.com/your-username/crypto-platform.git
cd crypto-platform
cp .env.example .env
```

Edit `.env`:

```env
GCP_PROJECT=your-gcp-project-id
BQ_DATASET_RAW=crypto_raw
BQ_DATASET_MART=crypto_mart
GCS_BUCKET=your-gcs-temp-bucket
BQ_LOCATION=EU
GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-credentials.json
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_RAW=crypto.raw.trades
KAFKA_TOPIC_METRICS=crypto.metrics
AIRFLOW_ADMIN_PASSWORD=password
```

### 2. Add GCP Credentials

Place your service account JSON key at:

```bash
secrets/gcp-credentials.json
```

The service account requires the following IAM roles:
- `BigQuery Data Editor`
- `BigQuery Job User`
- `Storage Object Admin`

### 3. Create BigQuery Datasets

```bash
bq mk --dataset --location=EU your-project:crypto_raw
bq mk --dataset --location=EU your-project:crypto_mart
```

### 4. Start the Platform

```bash
# Initialize Airflow DB and copy dbt files
docker compose up -d airflow-init
sleep 30

# Start all services
docker compose up -d

# Create Airflow admin user
docker exec airflow airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname Admin \
  --role Admin --email admin@crypto.com
```

---

## 📊 Data Pipeline

### Raw Data — `crypto_raw.trades`

Each row represents a single trade event from Binance:

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Trading pair (e.g. BTCUSDT) |
| `price` | FLOAT | Execution price |
| `quantity` | FLOAT | Trade quantity in base asset |
| `trade_time` | INTEGER | Trade timestamp (Unix ms) |
| `event_time` | INTEGER | Kafka event timestamp (Unix ms) |
| `is_buyer_market_maker` | BOOLEAN | True if the buyer is the market maker (sell-side aggressor) |
| `ingested_at` | STRING | UTC timestamp when the event was ingested |

### Streaming Metrics — `crypto_raw.metrics`

Computed by Spark every **30 seconds** over a **1-minute sliding window**:

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Trading pair |
| `vwap` | FLOAT | Volume Weighted Average Price = Σ(price × qty) / Σ(qty) |
| `volume` | FLOAT | Total traded quantity in the window |
| `trade_count` | INTEGER | Number of individual trades |
| `price_min` | FLOAT | Lowest price in the window |
| `price_max` | FLOAT | Highest price in the window |
| `price_open` | FLOAT | First price recorded in the window |
| `price_close` | FLOAT | Last price recorded in the window |
| `price_change_pct` | FLOAT | Percentage change: (close - open) / open × 100 |
| `window_start` | TIMESTAMP | Window start time |
| `window_end` | TIMESTAMP | Window end time |

---

## 🧱 dbt Models

### Staging (Views)

| Model | Source | Description |
|---|---|---|
| `stg_trades` | `crypto_raw.trades` | Cleaned trades with computed `trade_value`, typed timestamps |
| `stg_metrics` | `crypto_raw.metrics` | Cleaned metrics with rounded values and `price_range` |

### Mart (Tables — refreshed every 5 min by Airflow)

#### `mart_top_movers`

The latest 1-minute window per symbol, ranked by absolute price change:

| Column | Description |
|---|---|
| `symbol` | Trading pair |
| `vwap` | Volume Weighted Average Price |
| `volume` | Total volume in the window |
| `trade_count` | Number of trades |
| `price_change_pct` | Price variation % (negative = bearish) |
| `price_min / max` | Price range boundaries |
| `direction` | `UP` (change > 0), `DOWN` (change < 0), `FLAT` (no change) |
| `abs_change_pct` | Absolute value of change % (used for ranking) |
| `window_start / end` | Time boundaries of the window |

#### `mart_ohlcv_1h`

Hourly OHLCV candles aggregated from raw trades:

| Column | Description |
|---|---|
| `symbol` | Trading pair |
| `hour` | Candle timestamp (truncated to hour) |
| `trade_count` | Number of trades in the hour |
| `volume` | Total volume |
| `turnover` | Total traded value (price × qty) |
| `low / high` | Price range for the hour |
| `vwap` | Volume Weighted Average Price for the hour |

#### `mart_volume_heatmap`

Volume distribution by hour of day and day of week:

| Column | Description |
|---|---|
| `symbol` | Trading pair |
| `hour_of_day` | 0–23 (UTC) |
| `day_of_week` | 1 = Sunday, 7 = Saturday |
| `trade_count` | Number of trades in that slot |
| `volume` | Total volume in that slot |
| `avg_price` | Average price in that slot |

---

## 🚨 Alert System

Alerts are triggered by the `crypto_alerts` Airflow DAG, which runs **every 1 minutes**.

### Alert Conditions

| Condition | Threshold | Description |
|---|---|---|
| **Price Spike** | `ABS(price_change_pct) > 0.5%` | Symbol moved more than 0.5% in the last 5-minute window |

Alerts are visible in:
- The **Alerts panel** in the React dashboard (live via WebSocket)
- The **Airflow task logs** (task `log_alerts` prints a full report)

### Extending Alerts

The `log_alerts` task in `dag_alerts.py` can be extended to send notifications:

```python
# Slack
import requests
requests.post(SLACK_WEBHOOK_URL, json={"text": f"🚨 {symbol} spiked {pct}%"})

# Email via Airflow
from airflow.utils.email import send_email
send_email(to="you@email.com", subject="Crypto Alert", html_content=...)
```

---

## 🌐 API Reference

Base URL: `http://localhost:8000`

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/symbols` | List of available symbols |
| GET | `/top-movers?limit=10` | Top movers ranked by price change |
| GET | `/ohlcv/{symbol}?limit=24` | Hourly OHLCV candles for a symbol |
| GET | `/heatmap?symbol=BTCUSDT` | Volume heatmap data |
| GET | `/alerts?threshold=0.5` | Active alerts above threshold % |
| WS | `/ws/metrics` | WebSocket stream (pushes every 5s) |

### WebSocket Payload

```json
{
  "top_movers": [
    {
      "symbol": "BTCUSDT",
      "vwap": 74000.1234,
      "price_change_pct": -0.1559,
      "direction": "DOWN",
      "trade_count": 8382,
      "abs_change_pct": 0.1559,
      "window_end": "2026-03-18 17:35:00+00:00"
    }
  ],
  "alerts": [
    {
      "symbol": "SOLUSDT",
      "price_change_pct": -0.62,
      "vwap": 88.82,
      "direction": "DOWN",
      "window_end": "2026-03-18 17:35:00+00:00"
    }
  ]
}
```

---

## 📡 Services & Ports

| Service | URL | Description |
|---|---|---|
| React Dashboard | http://localhost:5173 | Live dashboard |
| FastAPI | http://localhost:8000 | REST + WebSocket API |
| Kafka UI | http://localhost:8080 | Kafka topic browser |
| Spark Master UI | http://localhost:8081 | Spark jobs monitor |
| Airflow | http://localhost:8082 | DAG orchestration (admin/admin) |

---

## 🔄 Airflow DAGs

### `dbt_refresh_mart` — Every 5 minutes

```
dbt_debug → dbt_run_staging → dbt_run_mart → dbt_test
```

Refreshes all dbt mart tables with the latest data from BigQuery RAW.

### `crypto_alerts` — Every 5 minutes

```
check_spikes → log_alerts
```

Queries `mart_top_movers` for symbols with `|price_change_pct| > 0.5%` and logs a detailed report.

---

## 📈 Key Metrics Explained

### VWAP (Volume Weighted Average Price)

```
VWAP = Σ(price × quantity) / Σ(quantity)
```

Unlike a simple average, VWAP weights each price by the volume traded at that price. It is the standard benchmark for institutional crypto trading — a price above VWAP is considered bullish, below is bearish.

### Price Change %

```
price_change_pct = (price_close - price_open) / price_open × 100
```

Computed over a **1-minute sliding window** refreshed every 30 seconds. Negative values indicate a downtrend, positive values an uptrend.

### Volume Heatmap

Shows trading activity intensity by **hour of day** (0–23 UTC) and **day of week**. Darker purple cells = higher volume. Useful for identifying peak trading hours.

---

## 🔒 Security Notes

- `secrets/` is gitignored — never commit GCP credentials
- The GCP service account uses the **principle of least privilege** (only BigQuery and Storage access)
- Kafka runs without authentication in dev — enable SASL/SSL for production
- Airflow uses a fixed `SECRET_KEY` — rotate it for production

---

## 🗺️ Roadmap

- [ ] Deploy full stack on VPS with Docker Compose
- [ ] Add more trading pairs (20+ symbols)
- [ ] Candlestick chart (replacing OHLCV bar chart)
- [ ] Slack/email notifications for alerts
- [ ] Historical backtesting with dbt snapshots
- [ ] Authentication on FastAPI (JWT)
- [ ] Kubernetes deployment with Helm charts

---

## 👤 Author

**Adrien Gutleben** — Backend Engineer pivoting to Data Engineering

- GitHub: [@agutleben](https://github.com/agutleben)
- Email: adrien.gutleben@gmail.com

---

## 📄 License

MIT License — feel free to use this project as a reference or starting point.
