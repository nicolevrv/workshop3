"""
producer.py
-----------
Reads the raw World Happiness CSV files (2015-2019) and sends each row
as a JSON event to the Kafka topic: happiness-predictions

Note: The producer reads the RAW files, not the processed/unified dataset.
      Cleaning and transformation are handled by the consumer.
"""

import json
import time
import pandas as pd
from kafka import KafkaProducer
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR  = BASE_DIR / 'data' / 'raw'

# ── Settings ───────────────────────────────────────────────────
KAFKA_BROKER = 'localhost:9092'
TOPIC        = 'happiness-predictions'
DELAY        = 0.5  # seconds between events

# Raw CSV files to stream (in order)
RAW_FILES = {
    2015: RAW_DIR / '2015.csv',
    2016: RAW_DIR / '2016.csv',
    2017: RAW_DIR / '2017.csv',
    2018: RAW_DIR / '2018.csv',
    2019: RAW_DIR / '2019.csv',
}

# Column mapping per year → unified field names
# The producer sends raw values with unified keys so the consumer
# can validate and clean them consistently.
COLUMN_MAPS = {
    2015: {
        'Country':                      'country',
        'Happiness Score':              'happiness_score',
        'Economy (GDP per Capita)':     'gdp',
        'Family':                       'family',
        'Health (Life Expectancy)':     'health',
        'Freedom':                      'freedom',
        'Trust (Government Corruption)':'corruption',
        'Generosity':                   'generosity',
    },
    2016: {
        'Country':                      'country',
        'Happiness Score':              'happiness_score',
        'Economy (GDP per Capita)':     'gdp',
        'Family':                       'family',
        'Health (Life Expectancy)':     'health',
        'Freedom':                      'freedom',
        'Trust (Government Corruption)':'corruption',
        'Generosity':                   'generosity',
    },
    2017: {
        'Country':                          'country',
        'Happiness.Score':                  'happiness_score',
        'Economy..GDP.per.Capita.':         'gdp',
        'Family':                           'family',
        'Health..Life.Expectancy.':         'health',
        'Freedom':                          'freedom',
        'Trust..Government.Corruption.':    'corruption',
        'Generosity':                       'generosity',
    },
    2018: {
        'Country or region':            'country',
        'Score':                        'happiness_score',
        'GDP per capita':               'gdp',
        'Social support':               'family',
        'Healthy life expectancy':      'health',
        'Freedom to make life choices': 'freedom',
        'Perceptions of corruption':    'corruption',
        'Generosity':                   'generosity',
    },
    2019: {
        'Country or region':            'country',
        'Score':                        'happiness_score',
        'GDP per capita':               'gdp',
        'Social support':               'family',
        'Healthy life expectancy':      'health',
        'Freedom to make life choices': 'freedom',
        'Perceptions of corruption':    'corruption',
        'Generosity':                   'generosity',
    },
}

# ── Initialize producer ────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f'Producer connected to {KAFKA_BROKER}')
print(f'Topic: {TOPIC}')
print('-' * 55)

# ── Stream raw files ───────────────────────────────────────────
total_sent   = 0
total_errors = 0

for year, filepath in RAW_FILES.items():
    print(f'\nStreaming {filepath.name} ...')

    df = pd.read_csv(filepath)
    mapping = COLUMN_MAPS[year]

    # Rename only the columns we care about
    df = df.rename(columns=mapping)

    sent   = 0
    errors = 0

    for _, row in df.iterrows():
        try:
            event = {
                "country":                str(row['country'])           if 'country'         in row.index and pd.notna(row['country'])         else None,
                "year":                   year,
                "gdp":                    float(row['gdp'])             if 'gdp'             in row.index and pd.notna(row['gdp'])             else None,
                "family":                 float(row['family'])          if 'family'          in row.index and pd.notna(row['family'])          else None,
                "health":                 float(row['health'])          if 'health'          in row.index and pd.notna(row['health'])          else None,
                "freedom":                float(row['freedom'])         if 'freedom'         in row.index and pd.notna(row['freedom'])         else None,
                "generosity":             float(row['generosity'])      if 'generosity'      in row.index and pd.notna(row['generosity'])      else None,
                "corruption":             float(row['corruption'])      if 'corruption'      in row.index and pd.notna(row['corruption'])      else None,
                "actual_happiness_score": float(row['happiness_score']) if 'happiness_score' in row.index and pd.notna(row['happiness_score']) else None,
            }

            producer.send(TOPIC, value=event)
            sent += 1
            total_sent += 1

            print(f'  [{total_sent:>4}] Sent: {event["country"]} ({year}) | score: {event["actual_happiness_score"]}')
            time.sleep(DELAY)

        except Exception as e:
            errors += 1
            total_errors += 1
            print(f'  [ERROR] Row {sent + errors} in {filepath.name}: {e}')

    print(f'  {filepath.name} completed - {sent} sent, {errors} errors')

# ── Flush and close ────────────────────────────────────────────
producer.flush()
producer.close()

print('\n' + '=' * 55)
print('Production completed.')
print(f'  Total events sent : {total_sent}')
print(f'  Total errors      : {total_errors}')