# World Bank Pipeline

An Airflow pipeline on Google Cloud (Cloud Composer) that fetches economic and social indicators from the World Bank API, stores the raw files in Cloud Storage, and loads them into BigQuery.

Built for the DataMaroc Data Engineering bootcamp (Airflow & GCP exercise).

## Architecture

```
World Bank API → extract_and_upload → CSV in Cloud Storage → load_to_bigquery → BigQuery table
```

- **Cloud Composer (Airflow 2.11)** orchestrates the two tasks.
- **Cloud Storage** holds the raw zone: `gs://<project>-worldbank-raw/worldbank/{{ ds }}/indicators.csv`
- **BigQuery** holds the table `raw_worldbank.indicators`.

## Data

- **Countries:** MAR, TUN, DZA, EGY, FRA
- **Indicators:** GDP (`NY.GDP.MKTP.CD`), inflation (`FP.CPI.TOTL.ZG`), population (`SP.POP.TOTL`), unemployment (`SL.UEM.TOTL.ZS`), life expectancy (`SP.DYN.LE00.IN`)

One row per country, indicator and year:

| Column | Type | Description |
|---|---|---|
| country_code | STRING | ISO3 country code |
| country_name | STRING | Country name |
| indicator_code | STRING | World Bank indicator code |
| indicator_label | STRING | Readable indicator name |
| year | INT64 | Year of the value |
| value | FLOAT64 | Indicator value |
| load_date | DATE | DAG execution date |

## The DAG

`worldbank_gdp_ingestion` runs `@weekly` with two tasks:

1. `extract_and_upload` (PythonOperator): calls the World Bank API, writes a temporary CSV, uploads it to Cloud Storage.
2. `load_to_bigquery` (GCSToBigQueryOperator): loads the file into `raw_worldbank.indicators` with `WRITE_TRUNCATE`.

## Design choices

- **Hidden API errors are detected.** The World Bank API can answer HTTP 200 with a JSON error message. The extraction checks the response shape and raises an error, so the task fails instead of loading nothing.
- **Retries on a slow API.** Each call is retried up to 3 times; the DAG also has `retries=1`.
- **Dated paths.** Each run writes to its own folder using the Airflow execution date.
- **Idempotent load.** `WRITE_TRUNCATE` replaces the table at each run, so re-running never creates duplicates.
- **Least privilege.** A dedicated service account has only: Composer Worker, BigQuery Data Editor, BigQuery Job User, and Storage Object Creator/Viewer on the bucket.

## Project structure

```
worldbank-pipeline/
├── dags/
│   └── worldbank_gdp_ingestion.py   # the Airflow DAG
├── test_extract.py                  # local test of the extraction
├── .gitignore
└── README.md
```

## Test the extraction locally

```bash
pip install requests
python test_extract.py
```

This calls the API for the 5 countries and 5 indicators and writes `indicators.csv`.

## Deploy

1. Create a GCP project with billing, enable the Cloud Composer, Cloud Storage, BigQuery and IAM Service Account Credentials APIs.
2. Create the bucket and the BigQuery dataset `raw_worldbank` in the same region (`europe-west9`).
3. Create a service account with the roles above and a Cloud Composer 3 environment that uses it.
4. Set `BUCKET_NAME` in the DAG, upload `dags/worldbank_gdp_ingestion.py` to the environment's DAG folder, then trigger the DAG from the Airflow UI.