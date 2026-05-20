"""
consumer.py
-----------
Reads events from Kafka topic: happiness-predictions
For each event:
  1. Store raw event in raw_happiness_events
  2. Validate event schema
  3. Load model.pkl and generate prediction
  4. Store prediction result in fact_predictions
"""

import json
import pickle
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from kafka import KafkaConsumer
import os

os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['PGCLIENTENCODING'] = 'UTF8'

# -- Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# -- Paths
BASE_DIR   = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / 'models' / 'model.pkl'

# -- Settings
KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'happiness-predictions'
GROUP_ID     = 'happiness-consumer-group'

DB_URL = 'postgresql+psycopg2://etl_user:etl_pass@127.0.0.1:5433/happiness_db'

REQUIRED_FIELDS  = ['country', 'year', 'gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
NUMERIC_FEATURES = ['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']

# -- Load model
log.info(f'Loading model from {MODEL_PATH}')
with open(MODEL_PATH, 'rb') as f:
    artifact = pickle.load(f)

model    = artifact['model']
features = artifact['features']
log.info(f'Model loaded. Features: {features}')

# -- Database engine
engine = create_engine(DB_URL)

# -- Insert raw event
def insert_raw_event(conn, raw_message: str, status: str) -> int:
    result = conn.execute(
        text("""
            INSERT INTO raw_happiness_events (raw_message, processing_status, received_at)
            VALUES (:msg, :status, :ts)
            RETURNING raw_event_id;
        """),
        {'msg': raw_message, 'status': status, 'ts': datetime.now(timezone.utc)}
    )
    raw_event_id = result.fetchone()[0]
    conn.commit()
    return raw_event_id

# -- Update raw event status
def update_raw_status(conn, raw_event_id: int, status: str):
    conn.execute(
        text("UPDATE raw_happiness_events SET processing_status = :status WHERE raw_event_id = :id;"),
        {'status': status, 'id': raw_event_id}
    )
    conn.commit()

# -- Validate event
def validate_event(event: dict) -> tuple[bool, str]:
    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        return False, f'INVALID_SCHEMA: missing fields {missing}'

    for field in NUMERIC_FEATURES:
        val = event.get(field)
        if val is None:
            return False, f'INVALID_VALUES: {field} is null'
        try:
            float(val)
        except (TypeError, ValueError):
            return False, f'INVALID_VALUES: {field} is not numeric'
        if float(val) < 0:
            return False, f'INVALID_VALUES: {field} is negative ({val})'

    return True, 'VALID'

# -- Get or create country dimension
def get_or_create_country(conn, country: str) -> int:
    row = conn.execute(
        text("SELECT country_id FROM dim_country WHERE country_name = :name;"),
        {'name': country}
    ).fetchone()
    if row:
        return row[0]
    result = conn.execute(
        text("INSERT INTO dim_country (country_name) VALUES (:name) RETURNING country_id;"),
        {'name': country}
    )
    country_id = result.fetchone()[0]
    conn.commit()
    return country_id

# -- Get or create date dimension
def get_or_create_date(conn, year: int) -> int:
    row = conn.execute(
        text("SELECT date_id FROM dim_date WHERE year = :year;"),
        {'year': year}
    ).fetchone()
    if row:
        return row[0]
    result = conn.execute(
        text("INSERT INTO dim_date (year) VALUES (:year) RETURNING date_id;"),
        {'year': year}
    )
    date_id = result.fetchone()[0]
    conn.commit()
    return date_id

# -- Insert prediction
def insert_prediction(conn, raw_event_id: int, country_id: int, date_id: int,
                      actual: float, predicted: float):
    error = abs(actual - predicted)
    conn.execute(
        text("""
            INSERT INTO fact_predictions
                (raw_event_id, country_id, date_id, actual_score, predicted_score,
                 prediction_error, prediction_timestamp)
            VALUES (:raw_id, :country_id, :date_id, :actual, :predicted, :error, :ts);
        """),
        {
            'raw_id': raw_event_id, 'country_id': country_id, 'date_id': date_id,
            'actual': actual, 'predicted': predicted, 'error': error,
            'ts': datetime.now(timezone.utc)
        }
    )
    conn.commit()

# -- Main consumer loop
def main():
    log.info(f'Connecting to Kafka broker: {KAFKA_BROKER}')
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    log.info(f'Listening on topic: {TOPIC}')
    log.info('=' * 60)

    processed = 0
    invalid   = 0
    errors    = 0

    with engine.connect() as conn:
        log.info('Connected to PostgreSQL via SQLAlchemy')

        for message in consumer:
            event        = message.value
            raw_str      = json.dumps(event)
            raw_event_id = None

            try:
                # Step 1 -- Store raw event
                raw_event_id = insert_raw_event(conn, raw_str, 'PENDING')

                # Step 2 -- Validate event
                is_valid, status = validate_event(event)

                if not is_valid:
                    update_raw_status(conn, raw_event_id, status.split(':')[0])
                    log.warning(f'[SKIP] {status} | raw_event_id={raw_event_id}')
                    invalid += 1
                    continue

                # Step 3 -- Build feature vector
                input_df = pd.DataFrame(
                    [[float(event[f]) for f in features]],
                    columns=features
                )

                # Step 4 -- Generate prediction
                predicted = float(model.predict(input_df)[0])
                actual    = float(event['actual_happiness_score'])

                # Step 5 -- Get/create dimensions
                country_id = get_or_create_country(conn, event['country'])
                date_id    = get_or_create_date(conn, int(event['year']))

                # Step 6 -- Store prediction
                insert_prediction(conn, raw_event_id, country_id, date_id, actual, predicted)

                # Step 7 -- Update raw status
                update_raw_status(conn, raw_event_id, 'VALID')

                processed += 1
                log.info(
                    f'[OK] {event["country"]} ({event["year"]}) | '
                    f'actual={actual:.3f} | predicted={predicted:.3f} | '
                    f'error={abs(actual - predicted):.3f}'
                )

            except Exception as e:
                errors += 1
                log.error(f'[ERROR] {e}')
                if raw_event_id:
                    try:
                        update_raw_status(conn, raw_event_id, 'PREDICTION_ERROR')
                    except Exception:
                        pass

    log.info('=' * 60)
    log.info(f'Consumer finished.')
    log.info(f'  Processed : {processed}')
    log.info(f'  Invalid   : {invalid}')
    log.info(f'  Errors    : {errors}')

if __name__ == '__main__':
    main()