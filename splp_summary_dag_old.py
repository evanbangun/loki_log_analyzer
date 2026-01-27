from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dremio_simple_query.connect import get_token, DremioConnection
import pandas as pd

# --------------------
# Config
# --------------------
DREMIO_HOST = "dremio-client.default.svc.cluster.local"
DREMIO_FLIGHT_PORT = 32010
DREMIO_LOGIN_PORT = 9047
DREMIO_USER = "admin"
DREMIO_PASS = "B1gd4t4_4dm1n"
POSTGRES_CONN_ID = "SPLP_postgres"
POSTGRES_TARGET_TABLE = "api_logs_summary"    # Postgres target table

# --------------------
# SQL template (placeholders for Python .format)
# --------------------
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

def get_data(execution_date=None, **context):
    today = datetime.now()

    # Latest FULL month (e.g. today=Dec → latest full=Nov)
    first_day_current = today.replace(day=1)
    latest_full_month = first_day_current - relativedelta(months=1)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    # --- Find the latest month in Postgres ---
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT MAX(date) FROM api_logs_summary;
        """).fetchone()

    last_processed_date = result[0]

    if last_processed_date is None:
        # Nothing processed yet → start from 2 years ago safety
        start_year = latest_full_month.year
        start_month = latest_full_month.month
    else:
        # Start from next month after last processed
        last_year = last_processed_date.year
        last_month = last_processed_date.month
        next_month = datetime(last_year, last_month, 1) + relativedelta(months=1)
        start_year = next_month.year
        start_month = next_month.month

    # Build month list to process
    to_process = []
    cursor = datetime(start_year, start_month, 1)

    while cursor <= latest_full_month:
        to_process.append(cursor)
        cursor += relativedelta(months=1)

    print("Months to process:")
    for m in to_process:
        print(" -", m.strftime("%Y-%m"))

    if not to_process:
        print("No missing months. Nothing to do.")
        return

    # --- Authenticate to Dremio ---
    token = get_token(
        uri=f"http://{DREMIO_HOST}:{DREMIO_LOGIN_PORT}/apiv2/login",
        payload={"userName": DREMIO_USER, "password": DREMIO_PASS},
    )
    dremio = DremioConnection(
        token,
        f"grpc://{DREMIO_HOST}:{DREMIO_FLIGHT_PORT}"
    )

    # --- Process each missing month ---
    for month_start in to_process:
        year = month_start.year
        month = month_start.month
        start_date = month_start
        end_date = start_date + relativedelta(months=1)

        print(f"\n=== Processing Month {year}-{month:02d} ===")

        sql = SQL_TEMPLATE.format(year=year, month=month)

        df: pd.DataFrame = dremio.toPandas(sql)
        print(f"Fetched {len(df)} rows from Dremio")

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.date

        with engine.begin() as conn:
            conn.execute(f"""
                DELETE FROM {POSTGRES_TARGET_TABLE}
                WHERE date >= '{start_date.date()}'
                AND date < '{end_date.date()}';
            """)
          
        if not df.empty:
            df.to_sql(POSTGRES_TARGET_TABLE, engine, if_exists="append", index=False)
          
        print(f"Inserted {len(df)} rows for {year}-{month:02d}")

# --------------------
# DAG definition
# --------------------
with DAG(
    dag_id="splp_summary_monthly",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 0 1 * *",  # run monthly on 1st at midnight
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
) as dag:

    run_etl = PythonOperator(
        task_id="run_splp_summary_monthly_etl",
        python_callable=get_data,
    )
