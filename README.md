# Real-Time Financial Fraud Detection Pipeline

A production-grade event-driven fraud detection system that processes a continuous stream of financial transactions, applies a two-tier AI anomaly detection engine, and streams live fraud alerts to analysts in real time.

Built as Portfolio Project 2 of 3 targeting **AI/Technical Solutions Architect** and **Staff/Principal Engineer** roles.

---

## Architecture

```
Transaction Producer (Python)
  └─► Kafka: transactions.raw (3 partitions)
        └─► Stream Processor (Python)
              ├─► Tier 1: Statistical Detection (every transaction, <1ms)
              │     ├─► Z-score on 50-transaction rolling window per account
              │     ├─► Velocity check (transactions per 60 seconds)
              │     ├─► Large amount threshold (>$5000 + 10x account mean)
              │     └─► Off-hours detection (2AM–4AM UTC)
              │           └─► Confidence score 0.0–1.0
              └─► Tier 2: GPT-4o-mini Escalation (confidence >= 0.75 only)
                    └─► is_fraud + risk_level + reasoning
                          ├─► PostgreSQL (transactions + alerts)
                          └─► Kafka: transactions.alerts
                                └─► FastAPI WebSocket → Grafana Dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 7.5 + Zookeeper |
| Stream Processing | Python 3.12 (kafka-python-ng) |
| Tier 1 Detection | Statistical (Z-score, velocity, threshold, off-hours) |
| Tier 2 Detection | OpenAI GPT-4o-mini |
| API | FastAPI + WebSocket |
| Database | PostgreSQL 16 |
| Migrations | Alembic |
| Visualization | Grafana 10 (auto-provisioned) |
| Containerization | Docker Compose (7 services) |

---

## Services

| Service | Image | Port | Role |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Message broker |
| producer | Python 3.12 | — | Transaction simulator |
| processor | Python 3.12 | — | Detection engine |
| api | FastAPI | 8001 | REST + WebSocket |
| db | PostgreSQL 16 | 5433 | Persistence |
| grafana | Grafana 10.2 | 3001 | Live dashboard |

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/health` | Health check + DB status |
| `GET` | `/api/v1/transactions` | Paginated transactions (filter by account, fraud flag) |
| `GET` | `/api/v1/transactions/{id}` | Single transaction + joined alert |
| `GET` | `/api/v1/alerts` | Paginated alerts (filter by risk level, detection tier) |
| `GET` | `/api/v1/stats` | Aggregate stats: TPS, alert rate, escalation count |
| `WS` | `/api/v1/alerts/live` | WebSocket — live fraud alert stream |

---

## Quick Start

### Prerequisites
- Docker Desktop with WSL2 backend (or Linux Docker)
- 4GB+ RAM allocated to Docker
- OpenAI API key with billing enabled

### 1. Clone and configure

```bash
git clone https://github.com/shruthiramesh19/fraud-detection-pipeline.git
cd fraud-detection-pipeline
cp .env.example .env
# Edit .env — add your OPENAI_API_KEY and set a POSTGRES_PASSWORD
```

### 2. Start infrastructure

```bash
docker compose up zookeeper kafka db -d
# Wait ~60 seconds for Kafka to become healthy
docker compose ps  # all three should show (healthy)
```

### 3. Create Kafka topics

```bash
docker compose exec kafka kafka-topics \
  --create --bootstrap-server localhost:9092 \
  --topic transactions.raw --partitions 3 --replication-factor 1 --if-not-exists

docker compose exec kafka kafka-topics \
  --create --bootstrap-server localhost:9092 \
  --topic transactions.alerts --partitions 1 --replication-factor 1 --if-not-exists
```

### 4. Run database migrations

```bash
docker run --rm \
  -v $(pwd)/api:/app \
  -v $(pwd)/alembic:/app/alembic \
  -v $(pwd)/alembic.ini:/app/alembic.ini \
  -v $(pwd)/.env:/app/.env \
  --network fraud-detection-pipeline_fraud-net \
  -e DATABASE_URL=postgresql://frauduser:yourpassword@db:5432/frauddb \
  python:3.12-slim bash -c "
    pip install sqlalchemy psycopg2-binary alembic pydantic pydantic-settings -q &&
    cd /app && alembic upgrade head
  "
```

### 5. Start application services

```bash
docker compose up producer processor api grafana --build -d
```

### 6. Verify

```bash
# API health
curl http://localhost:8001/api/v1/health

# Live stats
curl http://localhost:8001/api/v1/stats

# Open Grafana dashboard
open http://localhost:3001  # admin / admin123
```

---

## Fraud Detection Engine

### Detection Rules

| Rule | Logic | Confidence Weight |
|---|---|---|
| Z-score | Amount > 3.0 std devs above account rolling mean | 0.45 |
| Large amount | Amount > $5,000 AND > 10x account mean | 0.25 |
| Velocity | > 15 transactions in 60 seconds | 0.20 |
| Off-hours | Large purchase between 2AM–4AM UTC | 0.10 |

### Thresholds

- **Alert threshold:** confidence >= 0.35 (requires Z-score or multi-rule combination)
- **GPT escalation:** confidence >= 0.75 (multi-rule anomalies only, ~7% of alerts)

### Simulated Fraud Patterns

| Pattern | Trigger Rules | Amount |
|---|---|---|
| Large amount spike | Z-score + large amount | 8–15x account max |
| Foreign location | Z-score + category mismatch | 2–6x account max |
| Late night high-value | Off-hours + Z-score | 3–8x account max |
| Category mismatch | Z-score + amount | 4–10x account max (luxury/crypto) |

---

## Grafana Dashboard

Auto-provisioned at `http://localhost:3001` (admin / admin123) with 10 panels:

- **Stat panels:** Total Transactions, Total Alerts, GPT Escalations, Confirmed Fraud, Avg Confidence
- **Time series:** Alerts over time (last 1 hour, 10s refresh)
- **Pie charts:** Alerts by risk level, Detection tier breakdown
- **Bar chart:** Top flagged accounts
- **Table:** Recent alerts feed with merchant, location, GPT reasoning

---

## Project Structure

```
fraud-detection-pipeline/
├── producer/
│   ├── main.py          # Kafka producer loop + retry logic
│   ├── patterns.py      # Account profiles + fraud pattern generators
│   └── Dockerfile
├── processor/
│   ├── main.py          # Kafka consumer + orchestration
│   ├── detector.py      # Statistical detection (Z-score, velocity, threshold)
│   ├── escalator.py     # GPT-4o-mini escalation
│   ├── db.py            # PostgreSQL write layer (idempotent inserts)
│   └── Dockerfile
├── api/
│   ├── app/
│   │   ├── main.py      # FastAPI app + WebSocket endpoint + Kafka consumer thread
│   │   ├── core/        # Pydantic Settings
│   │   ├── db/          # SQLAlchemy engine + session
│   │   ├── models/      # Transaction + Alert ORM models
│   │   └── routes/      # health, transactions, alerts endpoints
│   └── Dockerfile
├── grafana/
│   └── provisioning/    # Auto-configured datasource + dashboard JSON
├── alembic/             # Database migrations
├── docker-compose.yml
└── .env.example
```

---

## Development

```bash
# View live logs from all services
docker compose logs -f producer processor api

# Check transaction + alert counts
docker compose exec db psql -U frauduser -d frauddb -c "
SELECT
  (SELECT COUNT(*) FROM transactions) AS transactions,
  (SELECT COUNT(*) FROM alerts) AS alerts,
  (SELECT COUNT(*) FROM alerts WHERE detection_tier = 'gpt') AS gpt_escalations;
"

# Rebuild a single service after code changes
docker compose up processor --build -d

# Reset everything (keeps DB volume)
docker compose down
docker compose up --build -d

# Full reset including database
docker compose down -v
```

---

## Design Decisions

**Why Kafka instead of direct DB writes?**
Kafka decouples producer from detector — producer never waits for detection to complete. Messages are durable and replayable (24h retention). At production scale, Kafka handles 100K+ msg/sec per broker.

**Why statistical rules + GPT instead of ML?**
Statistical rules handle 93% of decisions in <1ms at zero cost. GPT reserved for high-confidence cases needing explainable reasoning. Pure ML (Isolation Forest) would require labeled training data and retraining on new fraud patterns.

**Why PostgreSQL instead of a time-series DB?**
Transactions are structured relational data with account relationships. PostgreSQL handles our scale with proper indexing and monthly partitioning. TimescaleDB or InfluxDB would add operational complexity without meaningful benefit at this volume.

**Why WebSocket instead of polling for live alerts?**
Sub-second push delivery vs minimum 1-second polling interval. Single persistent connection vs N requests/second per client. Grafana's live panel connects via WebSocket for the real-time alert feed.

---

## Observed Performance (Development)

| Metric | Value |
|---|---|
| Transaction throughput | 2 TPS (configurable) |
| End-to-end alert latency | <1 second |
| Alert rate (after tuning) | ~5% of transactions |
| GPT escalation rate | ~7% of alerts |
| GPT accuracy | 100% confirmed fraud on escalated cases |
| DB growth rate | ~170KB/hour at 2 TPS |

---

## Portfolio Context

This is Project 2 of 3 in a portfolio targeting **AI/Technical Solutions Architect** and **Staff/Principal Engineer** roles:

1. **AI Financial Document Analyst** — FastAPI + RAG pipeline + pgvector + GPT-4o-mini
2. **Real-Time Fraud Detection Pipeline** ← this project
3. **AI Solutions Architect Tool** — Next.js + TypeScript + Claude API + Vercel (coming soon)
