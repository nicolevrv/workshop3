# Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01) — Data Engineering and Artificial Intelligence  
**Program:** Data Engineering and Artificial Intelligence  
**Dataset:** World Happiness Report 2015–2019

---

## Project Description

This project implements a streaming ETL pipeline that generates real-time happiness score predictions using Apache Kafka and a pre-trained machine learning model.

The pipeline reads historical World Happiness Report data (2015–2019), streams it through Kafka, validates each event, generates a prediction using a regression model, and stores both the raw event and the prediction result in a PostgreSQL database. A dashboard connected to the database visualizes the results through analytical KPIs.

---

## General Architecture

```
OFFLINE PROCESS
───────────────
Raw CSV Files (2015–2019)
    → EDA + Data Cleaning + Schema Harmonization  [eda.ipynb]
    → Feature Engineering + Model Training        [model_training.ipynb]
    → model.pkl

STREAMING PROCESS
─────────────────
Raw CSV Files
    → Kafka Producer      [kafka/producer.py]
    → Kafka Topic         [happiness-predictions]
    → Kafka Consumer      [kafka/consumer.py]
          → Store raw event         [raw_happiness_events]
          → Validate event schema
          → Load model.pkl
          → Generate prediction
          → Store prediction result [fact_predictions]
    → Dashboard & KPIs    [Looker Studio]
```

---

## Folder Structure

```
workshop3/
│
├── data/
│   ├── raw/                    # Original CSV files (2015–2019)
│   └── processed/              # Unified dataset after cleaning
│       └── happiness_unified.csv
│
├── notebooks/
│   ├── eda.ipynb               # EDA + Data Cleaning + Harmonization
│   └── model_training.ipynb    # Feature Engineering + Model Training
│
├── kafka/
│   ├── producer.py             # Streams CSV rows as Kafka events
│   └── consumer.py             # Receives events, predicts, stores in DB
│
├── models/
│   └── model.pkl               # Serialized trained model + feature list
│
├── sql/
│   ├── create_tables.sql       # DDL for all database tables
│   └── kpis.sql                # Analytical queries for dashboard KPIs
│
├── dashboards/                 # Dashboard screenshots and files
│
├── docker-compose.yml          # Kafka + Zookeeper + PostgreSQL
├── requirements.txt
└── README.md
```

---

## Data Cleaning Decisions

The five raw CSV files do not share the same schema. The following decisions were made during harmonization:

| Problem | Affected Years | Decision |
|---------|---------------|----------|
| Different column names for same concept | All | Unified mapping per year |
| Extra columns (rank, region, dystopia) | 2015, 2016, 2017 | Dropped — not relevant for modeling |
| Null values in `corruption` | 2018 | Imputed with median per year |
| Inconsistent data types | Some years | Cast to float64 via pandas |
| Mild outliers in generosity and corruption | All | Kept — real values from the report |

All cleaning decisions prioritize **data integrity** over data loss — rows are only dropped when the target variable (`happiness_score`) or the country identifier is missing.

---

## Feature Engineering Decisions

**Target variable:** `happiness_score`

**Selected features:**

| Feature | Justification |
|---------|--------------|
| `gdp` | Highest correlation with happiness (>0.8) |
| `family` | Strong social support predictor |
| `health` | High correlation with happiness score |
| `freedom` | Moderate-high correlation |
| `generosity` | Included for model completeness |
| `corruption` | Institutional perception — moderate correlation |

**Discarded features:**
- `country`: high-cardinality categorical variable, not useful for simple regression
- `year`: not a causal predictor of happiness

**No target leakage:** no feature is derived from `happiness_score`.

---

## Kafka Pipeline

### Producer (`kafka/producer.py`)
- Reads the 5 raw CSV files (2015–2019)
- Maps each year's columns to the unified schema
- Sends each row as a JSON event to the Kafka topic `happiness-predictions`
- Streams events one by one with a configurable delay

**Event format:**
```json
{
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2
}
```

### Consumer (`kafka/consumer.py`)
For each incoming event:
1. Stores the raw Kafka message in `raw_happiness_events` with status `PENDING`
2. Validates the event schema (required fields, numeric types, non-negative values)
3. Marks invalid events with the appropriate status (`INVALID_SCHEMA`, `INVALID_VALUES`) and skips prediction
4. Builds the feature vector in the correct order for the model
5. Generates a prediction using the loaded `model.pkl`
6. Gets or creates the country and date dimension records
7. Stores the prediction result in `fact_predictions`
8. Updates the raw event status to `VALID` or `PREDICTION_ERROR`

The pipeline **never crashes on invalid events** — all errors are caught, logged, and stored.

---

## Database Schema

```
raw_happiness_events          dim_country          dim_date
────────────────────          ───────────          ────────
raw_event_id (PK)             country_id (PK)      date_id (PK)
raw_message                   country_name         year
processing_status
received_at
        │
        │
        ▼
fact_predictions
────────────────
prediction_id (PK)
raw_event_id (FK) ──→ raw_happiness_events
country_id   (FK) ──→ dim_country
date_id      (FK) ──→ dim_date
actual_score
predicted_score
prediction_error
prediction_timestamp
```

**Processing status values:**
- `PENDING` — event received, not yet processed
- `VALID` — successfully predicted and stored
- `INVALID_SCHEMA` — missing required fields
- `INVALID_VALUES` — non-numeric or negative values
- `PREDICTION_ERROR` — model or database error during prediction

---

## Dashboard

The dashboard is built in **Looker Studio** connected directly to the PostgreSQL database. It queries live data — not CSV files.

**KPIs:**

| # | KPI | Description |
|---|-----|-------------|
| 1 | Average Prediction Error | Overall model performance (MAE, min, max, stddev) |
| 2 | Predictions by Country | Total predictions and average scores per country |
| 3 | Predicted vs Actual Score | Side-by-side comparison per country and year |
| 4 | Prediction Trends Over Time | Average scores and error per year |
| 5 | Top 10 Happiest Countries | Highest predicted happiness scores |
| 6 | Worst Predictions | Countries where the model had the highest error |

---

## Execution Instructions

### Prerequisites
- Docker Desktop running
- Python 3.x with virtual environment
- Dependencies installed: `pip install -r requirements.txt`

### 1. Start infrastructure
```bash
docker-compose up -d
```
Wait 15–20 seconds for Kafka and PostgreSQL to be ready.

### 2. Create database tables
```bash
docker exec -it postgres psql -U etl_user -d happiness_db -f /sql/create_tables.sql
```
Or paste the contents of `sql/create_tables.sql` directly into psql.

### 3. Run the EDA notebook
Open and run all cells in `notebooks/eda.ipynb`.  
This generates `data/processed/happiness_unified.csv`.

### 4. Run the model training notebook
Open and run all cells in `notebooks/model_training.ipynb`.  
This generates `models/model.pkl`.

### 5. Start the consumer
```bash
python kafka/consumer.py
```

### 6. Start the producer (in a new terminal)
```bash
python kafka/producer.py
```

The consumer will process each event as it arrives and store predictions in PostgreSQL.

### Notes for Windows users
If PostgreSQL is installed locally on Windows, it may conflict with Docker on port 5432. Change the Docker port mapping to `5433:5432` in `docker-compose.yml` and update `DB_URL` in `consumer.py` accordingly:
```python
DB_URL = 'postgresql+psycopg2://etl_user:etl_pass@127.0.0.1:5433/happiness_db'
```

---

## Technical Requirements

- Python 3.x
- Apache Kafka (via Docker)
- pandas
- scikit-learn
- SQLAlchemy + psycopg2
- PostgreSQL (via Docker)