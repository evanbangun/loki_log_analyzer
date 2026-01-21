import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dremio_simple_query.connect import get_token, DremioConnection
import pandas as pd
from sqlalchemy import text

# --------------------
# Config
# --------------------
DREMIO_HOST = "dremio-client.default.svc.cluster.local"
DREMIO_FLIGHT_PORT = 32010
DREMIO_LOGIN_PORT = 9047
DREMIO_USER = "admin"
DREMIO_PASS = "B1gd4t4_4dm1n"
POSTGRES_CONN_ID = "SPLP_postgres"
POSTGRES_TARGET_TABLE = "api_logs_summary"

logger = logging.getLogger(__name__)

SQL_TEMPLATE = """
SELECT 
    instansi_pemilik_api,
    instansi_requester_api,
    apiName AS apiname,
    CONCAT(LPAD("year", 4, '0'), '-', LPAD("month", 2, '0'), '-', LPAD("day", 2, '0')) AS "date",
    COUNT(*) AS records_count,
    SUM(CASE WHEN proxyResponseCode BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS total_proxy_success,
    SUM(CASE WHEN proxyResponseCode BETWEEN 300 AND 399 THEN 1 ELSE 0 END) AS total_proxy_3xx,
    SUM(CASE WHEN proxyResponseCode BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS total_proxy_4xx,
    SUM(CASE WHEN proxyResponseCode BETWEEN 500 AND 599 THEN 1 ELSE 0 END) AS total_proxy_5xx,
    SUM(CASE WHEN targetResponseCode = -1 AND proxyResponseCode BETWEEN 300 AND 399 THEN 1 ELSE 0 END) AS total_gateway_3xx,
    SUM(CASE WHEN targetResponseCode = -1 AND proxyResponseCode BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS total_gateway_4xx,
    SUM(CASE WHEN targetResponseCode = -1 AND proxyResponseCode BETWEEN 500 AND 599 THEN 1 ELSE 0 END) AS total_gateway_5xx,
    SUM(responseLatency) AS sum_latency,
    MAX(responseLatency) AS max_latency,
    MIN(responseLatency) AS min_latency
FROM
    "@admin"."splp-logs-joined"
WHERE
    "year" = {year}
    AND "month" = {month}
    AND "day" = {day}
GROUP BY
    instansi_requester_api, 
    instansi_pemilik_api, 
    apiname, 
    "year", 
    "month", 
    "day"
ORDER BY 
    "date", 
    instansi_pemilik_api, 
    instansi_requester_api, 
    apiname
"""

def get_data(**context):
    """
    Process daily summary data from Dremio and load into Postgres.
    
    Args:
        **context: Airflow context (contains data_interval_start, logical_date, etc.)
    """
    try:
        # Use data_interval_start (Airflow 2.x+) or logical_date as fallback
        # execution_date is deprecated in Airflow 3.x
        logical_date = context.get("data_interval_start") or context.get("logical_date")
        
        if logical_date is None:
            # Fallback to current date if not provided (e.g., manual trigger without date)
            logical_date = datetime.now()
            logger.warning("No data_interval_start or logical_date in context, using current date")
        
        # Handle timezone-aware datetime objects
        if hasattr(logical_date, 'tzinfo') and logical_date.tzinfo is not None:
            logical_date = logical_date.replace(tzinfo=None)
        
        if isinstance(logical_date, str):
            logical_date = datetime.fromisoformat(logical_date.replace("Z", "+00:00"))
        
        # For daily DAG, data_interval_start represents D-1 (yesterday)
        # e.g., if DAG runs on Jan 19, data_interval_start is Jan 18 00:00:00
        target_date = logical_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"Processing summary data for date: {target_date.strftime('%Y-%m-%d')} (D-1)")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        # --- Find the latest day in Postgres ---
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT MAX(date) FROM api_logs_summary")).fetchone()
            last_processed_date = result[0] if result else None
        except Exception as e:
            logger.warning(f"Error querying Postgres for last processed date: {e}. Assuming no data exists.")
            last_processed_date = None

        # For normal scheduled runs, process only the target date (D-1)
        # For backfill/catchup, process all missing days
        if last_processed_date is None:
            # Nothing processed yet → start from 30 days ago for backfill
            start_date = target_date - timedelta(days=30)
            logger.info(f"No existing data found. Starting backfill from 30 days ago: {start_date.strftime('%Y-%m-%d')}")
        else:
            # Check if we need to backfill or just process today's D-1
            if isinstance(last_processed_date, str):
                last_processed_date = datetime.fromisoformat(last_processed_date).date()
            elif hasattr(last_processed_date, 'date'):
                last_processed_date = last_processed_date.date()
            
            # Convert to datetime for comparison
            if isinstance(last_processed_date, datetime):
                last_processed_dt = last_processed_date.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                last_processed_dt = datetime.combine(last_processed_date, datetime.min.time())
            
            # If last processed is before target_date, we need to backfill
            if last_processed_dt < target_date:
                start_date = last_processed_dt + timedelta(days=1)
                logger.info(f"Last processed date: {last_processed_date}. Backfilling from: {start_date.strftime('%Y-%m-%d')} to {target_date.strftime('%Y-%m-%d')}")
            else:
                # Already processed up to or past target_date, just process target_date
                start_date = target_date
                logger.info(f"Last processed date: {last_processed_date}. Processing target date: {target_date.strftime('%Y-%m-%d')}")

        # Build day list to process (from start_date to target_date)
        to_process = []
        cursor = start_date

        while cursor <= target_date:
            to_process.append(cursor)
            cursor += timedelta(days=1)

        logger.info(f"Days to process: {len(to_process)} days from {to_process[0].strftime('%Y-%m-%d') if to_process else 'N/A'} to {to_process[-1].strftime('%Y-%m-%d') if to_process else 'N/A'}")

        if not to_process:
            logger.info("No missing days. Nothing to do.")
            return

        # --- Authenticate to Dremio ---
        try:
            token = get_token(
                uri=f"http://{DREMIO_HOST}:{DREMIO_LOGIN_PORT}/apiv2/login",
                payload={"userName": DREMIO_USER, "password": DREMIO_PASS},
            )
            dremio = DremioConnection(
                token,
                f"grpc://{DREMIO_HOST}:{DREMIO_FLIGHT_PORT}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Dremio: {e}")
            raise

        # --- Process each missing day ---
        total_rows_processed = 0
        for day_start in to_process:
            year = day_start.year
            month = day_start.month
            day = day_start.day
            start_date = day_start
            end_date = start_date + timedelta(days=1)

            logger.info(f"Processing Day {year}-{month:02d}-{day:02d}")

            try:
                sql = SQL_TEMPLATE.format(year=year, month=month, day=day)
                df: pd.DataFrame = dremio.toPandas(sql)
                logger.info(f"Fetched {len(df)} rows from Dremio for {year}-{month:02d}-{day:02d}")

                if not df.empty:
                    df["date"] = pd.to_datetime(df["date"]).dt.date

                # Use parameterized query to prevent SQL injection
                with engine.begin() as conn:
                    # Delete existing data for this month (idempotent operation)
                    # Note: Table name is a constant, so f-string is safe here
                    delete_stmt = text(
                        f'DELETE FROM "{POSTGRES_TARGET_TABLE}" WHERE date >= :start_date AND date < :end_date'
                    )
                    conn.execute(delete_stmt, {"start_date": start_date.date(), "end_date": end_date.date()})
                    logger.info(f"Deleted existing data for {year}-{month:02d}-{day:02d}")

                if not df.empty:
                    df.to_sql(POSTGRES_TARGET_TABLE, engine, if_exists="append", index=False, method="multi")
                    total_rows_processed += len(df)
                    logger.info(f"Inserted {len(df)} rows for {year}-{month:02d}-{day:02d}")
                else:
                    logger.warning(f"No data found for {year}-{month:02d}-{day:02d}")

            except Exception as e:
                logger.error(f"Error processing day {year}-{month:02d}-{day:02d}: {e}", exc_info=True)
                # Continue with next day instead of failing completely
                continue

        logger.info(f"Summary ETL completed. Total rows processed: {total_rows_processed}")

    except Exception as e:
        logger.error(f"Fatal error in summary ETL: {e}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"Fatal error in summary ETL: {e}", exc_info=True)
        raise

# --------------------
# DAG definition
# --------------------
with DAG(
    dag_id="splp_summary_daily",
    description="Daily summary ETL: Aggregate API logs from Dremio and load into Postgres",
    start_date=datetime(2025, 1, 1),
    schedule="0 10 * * *",  # run daily at 10 AM (3 hours after loki_ndjson_to_parquet_daily at 7 AM)
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["splp", "summary", "daily", "dremio", "postgres"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_splp_summary_daily_etl",
        python_callable=get_data,
    )
