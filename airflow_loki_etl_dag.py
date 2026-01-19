from __future__ import annotations

from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow import DAG  # type: ignore[import-not-found]
from airflow.operators.python import PythonOperator  # type: ignore[import-not-found]

from airflow_loki_ndjson_ingest import ingest_loki_to_ndjson
from airflow_ndjson_to_parquet import convert_ndjson_to_parquet


def _run_ingest(**context) -> None:
    window_start = context["data_interval_start"]
    window_end = context["data_interval_end"]
    ingest_loki_to_ndjson(window_start=window_start, window_end=window_end)


def _run_convert(**context) -> None:
    window_start = context["data_interval_start"]
    window_end = context["data_interval_end"]
    convert_ndjson_to_parquet(window_start=window_start, window_end=window_end)


with DAG(
    dag_id="loki_ndjson_to_parquet_daily",
    description="Ingest Loki logs to NDJSON and convert to Parquet daily.",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=True,  # Set to True to automatically process missed dates
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["loki", "etl", "minio"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_loki_ndjson",
        python_callable=_run_ingest,
    )

    convert_task = PythonOperator(
        task_id="convert_ndjson_parquet",
        python_callable=_run_convert,
    )

    ingest_task >> convert_task
