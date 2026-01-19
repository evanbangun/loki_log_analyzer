import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore[import-not-found]

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = (10, 600)
HOUR_STEP = timedelta(hours=1)
DEFAULT_LOOKBACK_DAYS = 1
NANOSECONDS_IN_SECOND = 1_000_000_000
HOUR_STEP_NS = int(HOUR_STEP.total_seconds() * NANOSECONDS_IN_SECOND)

LOKI_URL = "http://10.31.67.116:3100/loki/api/v1/query_range"
QUERY = '{container="splp-gw"} |~ "Metric Name: apim:response"'
LIMIT = 5000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_CONN_ID = "SPLP_minio"
MINIO_BUCKET = "splp-logs-raw"
MINIO_PREFIX = "year={year}/month={month}/day={day}"
TMP_LOCAL_DIR = "/tmp/splp-logs-raw"


def _build_session() -> Session:
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _datetime_to_ns(value: datetime) -> int:
    return int(value.replace(tzinfo=timezone.utc).timestamp() * NANOSECONDS_IN_SECOND)


def resolve_window(
    *,
    logical_date: Optional[datetime],
    window_start: Optional[datetime],
    window_end: Optional[datetime],
    default_lookback_days: int = 1,
) -> Tuple[datetime, datetime]:
    if window_start and window_end:
        start = window_start.replace(tzinfo=None)
        end = window_end.replace(tzinfo=None)
        return start, end

    if logical_date:
        start = (logical_date - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )
        end = start + timedelta(days=1)
        return start, end

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start = (now - timedelta(days=default_lookback_days)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = start + timedelta(days=1)
    return start, end


def _build_minio_keys(day: datetime) -> Tuple[str, str]:
    prefix = MINIO_PREFIX.format(year=day.year, month=day.month, day=day.day)
    final_key = f"{prefix}/logs.ndjson"
    tmp_key = f"tmp/{prefix}/logs.ndjson"
    return tmp_key, final_key


def _upload_to_minio(local_path: str, tmp_key: str, final_key: str) -> None:
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    client = s3.get_conn()
    s3.load_file(
        filename=local_path,
        key=tmp_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )
    client.copy_object(
        Bucket=MINIO_BUCKET,
        CopySource={"Bucket": MINIO_BUCKET, "Key": tmp_key},
        Key=final_key,
    )
    client.delete_object(Bucket=MINIO_BUCKET, Key=tmp_key)
    logger.info("Uploaded NDJSON to s3://%s/%s", MINIO_BUCKET, final_key)


def ingest_loki_to_ndjson(
    logical_date: Optional[datetime] = None,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> int:
    start_dt, end_dt = resolve_window(
        logical_date=logical_date,
        window_start=window_start,
        window_end=window_end,
        default_lookback_days=DEFAULT_LOOKBACK_DAYS,
    )

    tmp_key, final_key = _build_minio_keys(start_dt)
    local_dir = os.path.join(
        TMP_LOCAL_DIR,
        f"year={start_dt.year}",
        f"month={start_dt.month}",
        f"day={start_dt.day}",
    )
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "logs.ndjson")
    tmp_path = f"{local_path}.tmp"
    total_records = 0
    total_success = 0
    total_fail = 0

    session = _build_session()
    with open(tmp_path, "w", encoding="utf-8", buffering=1024 * 1024) as out:
        current_start = start_dt
        current_end = min(current_start + HOUR_STEP, end_dt)
        current_start_ns = _datetime_to_ns(current_start)
        current_end_ns = _datetime_to_ns(current_end)
        end_ns = _datetime_to_ns(end_dt)

        while current_start_ns < end_ns:
            params = {
                "query": QUERY,
                "start": str(current_start_ns),
                "end": str(current_end_ns),
                "limit": LIMIT,
                "direction": "FORWARD",
            }

            response = session.get(LOKI_URL, params=params, timeout=DEFAULT_TIMEOUT)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Loki query failed: {response.status_code} {response.text}"
                )

            data = response.json()
            logs_count = 0
            max_time_ns: Optional[int] = None
            for stream in data.get("data", {}).get("result", []):
                for value in stream.get("values", []):
                    try:
                        ts_ns = int(value[0])
                        if max_time_ns is None or ts_ns > max_time_ns:
                            max_time_ns = ts_ns
                    except Exception as exc:
                        total_fail += 1
                        logger.exception("Skipping record due to parse error: %s", exc)
                        continue
                    out.write(value[1] + "\n")
                    logs_count += 1
                    total_success += 1

            total_records += logs_count
            
            # If no results, move to next hour
            if max_time_ns is None or logs_count == 0:
                current_start_ns = current_end_ns
                current_end_ns = min(current_start_ns + HOUR_STEP_NS, end_ns)
                continue
            
            # If we got fewer results than the limit, we've exhausted this time window
            # Move to next hour
            if logs_count < LIMIT:
                current_start_ns = current_end_ns
                current_end_ns = min(current_start_ns + HOUR_STEP_NS, end_ns)
                continue
            
            # If we hit the limit (logs_count == LIMIT), we need to paginate
            # If max_time_ns is still within the current hour, continue querying from max_time_ns + 1
            # within the SAME hour window
            if max_time_ns < current_end_ns:
                # Continue paginating within the same hour
                current_start_ns = max_time_ns + 1
                # Keep current_end_ns the same (stay in same hour)
                continue
            
            # If max_time_ns >= current_end_ns, we've reached or exceeded the hour boundary
            # Move to next hour
            current_start_ns = current_end_ns
            current_end_ns = min(current_start_ns + HOUR_STEP_NS, end_ns)

    os.replace(tmp_path, local_path)
    _upload_to_minio(local_path, tmp_key, final_key)
    total_records = total_success + total_fail
    print(
        "NDJSON ingest complete: "
        f"total_records={total_records}, "
        f"total_success={total_success}, "
        f"total_fail={total_fail}"
    )
    return total_records
