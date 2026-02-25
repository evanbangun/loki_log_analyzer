from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import io
import json
import requests

# -------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------
POSTGRES_CONN_ID = "SPLP_postgres"
MINIO_CONN_ID = "SPLP_minio"     # Configure in Airflow Connections (S3 type with MinIO endpoint)
BUCKET = "ippd-all"         # Target MinIO bucket
FINAL_KEY = "ippd_all.parquet"     # Final atomic "current" file
TMP_KEY = "tmp/ippd_all.parquet"   # Temporary upload path
TMP_LOCAL = "/tmp/ippd_all.parquet"

# -------------------------------------------------------
# DAG CONFIG
# -------------------------------------------------------
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="sync_master_data_full_overwrite",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
)

# -------------------------------------------------------
# TASK 1: Extract full table from PostgreSQL
# -------------------------------------------------------
def extract_full_postgres(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
        SELECT *
        FROM public.ippd_all
    """
    df = pg.get_pandas_df(sql)

    if df.empty:
        raise ValueError("Master data table (ippd_all) returned EMPTY result. Aborting overwrite to prevent 0-byte dataset.")
  
    table = pa.Table.from_pandas(df)
    pq.write_table(table, TMP_LOCAL, compression="snappy")

extract_task = PythonOperator(
    task_id="extract_full_postgres",
    python_callable=extract_full_postgres,
    dag=dag,
)

# -------------------------------------------------------
# TASK 2: Upload to MinIO (atomic swap)
# -------------------------------------------------------
def upload_atomic(**context):
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    client = s3.get_conn()

    # Step A: Upload to tmp/
    s3.load_file(
        filename=TMP_LOCAL,
        key=TMP_KEY,
        bucket_name=BUCKET,
        replace=True
    )

    # Step B: Copy to final path (atomic publish)
    client.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": TMP_KEY},
        Key=FINAL_KEY,
    )

    # Step C: Cleanup tmp
    client.delete_object(Bucket=BUCKET, Key=TMP_KEY)

upload_task = PythonOperator(
    task_id="upload_atomic",
    python_callable=upload_atomic,
    dag=dag,
)


# -------------------------------------------------------
# DAG FLOW
# -------------------------------------------------------
extract_task >> upload_task
