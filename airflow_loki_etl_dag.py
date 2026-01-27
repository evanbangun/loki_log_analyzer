from __future__ import annotations

import logging
from datetime import timedelta

import pendulum  # type: ignore[import-not-found]
from airflow import DAG  # type: ignore[import-not-found]
from airflow.operators.python import PythonOperator  # type: ignore[import-not-found]

from airflow_loki_ndjson_ingest import ingest_loki_to_ndjson
from airflow_ndjson_to_parquet import convert_ndjson_to_parquet
from dremio_simple_query.connect import get_token, DremioConnection

logger = logging.getLogger(__name__)

# Dremio configuration
DREMIO_HOST = "dremio-client.default.svc.cluster.local"
DREMIO_FLIGHT_PORT = 32010
DREMIO_LOGIN_PORT = 9047
DREMIO_USER = "admin"
DREMIO_PASS = "B1gd4t4_4dm1n"

def _refresh_dremio_metadata(**context) -> None:
    """
    Refresh Dremio metadata after parquet upload to make new partitions immediately queryable.
    
    Args:
        **context: Airflow context
    """
    try:
        logger.info("Connecting to Dremio for metadata refresh...")
        token = get_token(
            uri=f"http://{DREMIO_HOST}:{DREMIO_LOGIN_PORT}/apiv2/login",
            payload={"userName": DREMIO_USER, "password": DREMIO_PASS},
        )
        dremio = DremioConnection(
            token,
            f"grpc://{DREMIO_HOST}:{DREMIO_FLIGHT_PORT}"
        )
        
        # Refresh the base table to discover new partitions
        refresh_sql = 'ALTER TABLE "minio"."splp-logs" REFRESH METADATA'
        logger.info("Refreshing Dremio metadata for minio.splp-logs table...")
        dremio.toPandas(refresh_sql)
        logger.info("Dremio metadata refresh completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to refresh Dremio metadata: {e}", exc_info=True)
        raise


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

    refresh_metadata_task = PythonOperator(
        task_id="refresh_dremio_metadata",
        python_callable=_refresh_dremio_metadata,
    )

    ingest_task >> convert_task >> refresh_metadata_task
