# Ashpaz: Real-Time Food Delivery Streaming Pipeline

A scalable, end-to-end data pipeline for a real-time food ordering and delivery platform. The system transitions from relational database designs to live stream processing, combining SQL analytics, Apache Kafka event streaming, and PySpark batch and real-time processing into a unified Lambda-style architecture.

## Architecture

```
zomato.csv ──┬──> [Ashpaz Producer] ──> Kafka: ashpaz.order
              │                              │
              │                         [Kafka Validator]
              │                         ┌────┴────┐
              │                  ashpaz.valid   ashpaz.error_log
              │                         │
              │              [Spark Streaming Fraud Detection]
              │                         │
              │                  orders.fraud_alerts
              │
              └──> [PostgreSQL] ──> SQL Analytics (Q1)

historical_data.json ──> [Spark Batch Analytics] ──> Insights (Q3a)
```

## Tech Stack

| Component | Technology |
|---|---|
| Event Streaming | Apache Kafka 3.9 (KRaft mode) |
| Stream Processing | PySpark Structured Streaming |
| Batch Processing | PySpark |
| Database | PostgreSQL |
| Kafka Client | confluent-kafka (Python) |
| Language | Python 3.12, SQL |

## Project Structure

```
.
├── src/
│   ├── db/
│   │   ├── schema.sql                # DDL: normalized table definitions
│   │   ├── data_loader_queries.sql   # Data loading + analytics queries
│   │   └── import_zomato.py          # ETL: CSV → PostgreSQL
│   ├── kafka/
│   │   ├── Ashpaz.py                 # Kafka producer (order generator)
│   │   └── kafka_validator.py        # Stateless order validator
│   └── spark/
│       ├── spark_batch_analytics.py  # Batch analytics on historical data
│       └── spark_streaming_fraud.py  # Real-time fraud detection
├── zomato.csv                        # Restaurant dataset (12,429 records)
├── historical_data.json              # Historical order events (~38K records)
├── config/                           # Configuration files
├── jars/                             # Spark JAR dependencies
├── requirements.txt                  # Python dependencies
└── README.md
```

## Prerequisites & Installation

### Java 17

Java is required by PySpark.

```bash
# Ubuntu/Debian
sudo apt update && sudo apt install -y openjdk-17-jdk

# macOS (Homebrew)
brew install openjdk@17

# Verify
java -version
```

### Python Dependencies

```bash
pip install -r requirements.txt
```

This installs PySpark, confluent-kafka (Kafka Python client), psycopg2-binary (PostgreSQL adapter), and visualization libraries (pandas, plotly, matplotlib, seaborn).

### PostgreSQL

```bash
# Ubuntu/Debian
sudo apt install -y postgresql
sudo systemctl start postgresql
sudo systemctl enable postgresql

# macOS (Homebrew)
brew install postgresql@16
brew services start postgresql@16
```

Create the project database and user:

```bash
sudo -u postgres psql -c "CREATE USER ashpaz_user WITH PASSWORD 'password123';"
sudo -u postgres psql -c "CREATE DATABASE ashpaz_db OWNER ashpaz_user;"
sudo -u postgres psql -d ashpaz_db -c "GRANT ALL ON SCHEMA public TO ashpaz_user;"
```

### Apache Kafka (KRaft mode)

Modern Kafka (3.3+) uses KRaft mode and no longer requires ZooKeeper.

```bash
# Download and extract
cd /tmp
curl -L "https://dlcdn.apache.org/kafka/3.9.2/kafka_2.13-3.9.2.tgz" -o kafka.tgz
tar -xzf kafka.tgz
export KAFKA_HOME=/tmp/kafka_2.13-3.9.2

# Initialize KRaft storage
KAFKA_CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
$KAFKA_HOME/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID \
    -c $KAFKA_HOME/config/kraft/server.properties

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon \
    $KAFKA_HOME/config/kraft/server.properties

# Verify
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Create the required topics:

```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic ashpaz.order --partitions 1 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic ashpaz.valid --partitions 1 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic ashpaz.error_log --partitions 1 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic orders.fraud_alerts --partitions 1 --replication-factor 1
```

---

## Q1: Database Design & Analytics

Normalized relational schema built from a flat Zomato restaurant CSV into three tables with proper primary/foreign key relationships.

### Schema

- **`locations`** — location_id (PK), location_name, listed_in_city
- **`restaurants`** — restaurant_id (PK), name, address, phone, location_id (FK)
- **`restaurant_metrics_services`** — metric_id (PK), restaurant_id (FK), online_order, book_table, rate, votes, rest_type, cuisines, approx_cost_two, menu_item, listed_in_type

### Run

```bash
# 1. Create tables
psql -h localhost -U ashpaz_user -d ashpaz_db -f src/db/schema.sql

# 2. Load data (CSV → staging → normalized tables)
python3 src/db/import_zomato.py

# 3. Run analytics queries
psql -h localhost -U ashpaz_user -d ashpaz_db -f src/db/data_loader_queries.sql
```

### Analytics Queries

1. Total delivery restaurants and average cost for two
2. Online ordering effect on pricing, by city
3. Top-tier dining neighborhoods (avg rating > 4.2, 50+ restaurants)
4. Pricing tier dashboard (Budget / Mid-Range / Premium)

---

## Q2: Kafka Consumer Implementation

Stateless order validation consuming from `ashpaz.order`. Each order is validated against three business rules without any database access:

| Rule | Logic | Error Type |
|---|---|---|
| Phone Format | Must start with `+91` or `080` | `INVALID_PHONE` |
| Order Mode | Cannot have both `request_online` and `request_table` true | `ORDER_MODE_CONFLICT` |
| Price Integrity | `order_price` must equal `Σ(unit_price × quantity)` | `PRICE_MISMATCH` |

Valid orders → `ashpaz.valid`. Invalid orders → `ashpaz.error_log` with structured error payload.

### Run

```bash
# Terminal 1: Validator
python3 src/kafka/kafka_validator.py

# Terminal 2: Producer (generates order events)
python3 src/kafka/Ashpaz.py
```

---

## Q3: PySpark Processing Layer

### Batch Analytics

Processes `historical_data.json` with PySpark to produce:

- **Revenue & Menu Analysis** — Top ordered items, highest revenue restaurants, online vs. dine-in leaders
- **Order Time & Peak Hour Analysis** — 24-hour order distribution, peak/off-peak identification
- **Geographic Analysis** — Revenue by city, regional favorite foods per city

```bash
python3 src/spark/spark_batch_analytics.py
```

### Real-Time Fraud Detection

Spark Structured Streaming application consuming from `ashpaz.valid` with time-based windowing and checkpointing:

- **Geographical Impossibility** — Flags same user ordering from 2+ cities within 30 simulated minutes
- **Velocity / Spam Detection** — Flags same user placing >5 orders within 60 simulated minutes
- **Alert Generation** — Writes fraud alerts to `orders.fraud_alerts` Kafka topic

```bash
# Terminal 1: Producer
python3 src/kafka/Ashpaz.py

# Terminal 2: Validator
python3 src/kafka/kafka_validator.py

# Terminal 3: Streaming fraud detection
rm -rf spark_checkpoints/fraud_detection
python3 src/spark/spark_streaming_fraud.py
```

## Time Acceleration

The producer simulates time at 5x speed (1 real second = 5 simulated minutes) to make temporal patterns observable without extended runtime. Adjust via `TIME_ACCELERATION` environment variable.

## Dataset

- **`zomato.csv`** — 12,429 Bangalore restaurant records with ratings, cuisines, pricing, and menu data
- **`historical_data.json`** — ~38,411 historical order events in JSONL format with intentionally injected errors for validation testing
