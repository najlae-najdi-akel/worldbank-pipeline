import csv
import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage

BUCKET_NAME = "worldbank-pipeline-515223-worldbank-raw"
BASE_URL = "https://api.worldbank.org/v2"

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = [
    "NY.GDP.MKTP.CD",
    "FP.CPI.TOTL.ZG",
    "SP.POP.TOTL",
    "SL.UEM.TOTL.ZS",
    "SP.DYN.LE00.IN",
]
COLUMNS = [
    "country_code", "country_name", "indicator_code",
    "indicator_label", "year", "value", "load_date",
]


def fetch_indicator(country_code, indicator_code):
    url = f"{BASE_URL}/country/{country_code}/indicator/{indicator_code}"
    params = {"format": "json", "per_page": 100}

    payload = None
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            break
        except requests.exceptions.RequestException as error:
            print(f"Essai {attempt + 1}/3 échoué pour {country_code}/{indicator_code} : {error}")
            time.sleep(5)
    if payload is None:
        raise RuntimeError(f"L'API ne répond pas pour {country_code}/{indicator_code}")

    if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
        raise ValueError(f"Erreur API pour {country_code}/{indicator_code}: {payload}")

    rows = []
    for item in payload[1]:
        rows.append({
            "country_code": item["countryiso3code"],
            "country_name": item["country"]["value"],
            "indicator_code": item["indicator"]["id"],
            "indicator_label": item["indicator"]["value"],
            "year": int(item["date"]),
            "value": item["value"],
        })
    return rows


def extract_all(load_date):
    all_rows = []
    for country in COUNTRIES:
        for indicator in INDICATORS:
            rows = fetch_indicator(country, indicator)
            for row in rows:
                if row["value"] is None:
                    continue
                row["load_date"] = load_date
                all_rows.append(row)
    return all_rows


def write_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def extract_and_upload_to_gcs(ds, **_):
    rows = extract_all(ds)

    local_path = "/tmp/indicators.csv"
    write_csv(rows, local_path)

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"worldbank/{ds}/indicators.csv")
    blob.upload_from_filename(local_path)

    print(f"{len(rows)} lignes envoyées vers gs://{BUCKET_NAME}/worldbank/{ds}/indicators.csv")


with DAG(
    dag_id="worldbank_gdp_ingestion",
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "data-eng-bootcamp", "retries": 1},
) as dag:

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload_to_gcs,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["worldbank/{{ ds }}/indicators.csv"],
        destination_project_dataset_table="raw_worldbank.indicators",
        schema_fields=[
            {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "year", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "load_date", "type": "DATE", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
    )

    extract_and_upload >> load_to_bigquery