import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore[import-not-found]
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_LOOKBACK_DAYS = 1
BATCH_SIZE = 5000
RETENTION_DAYS = 30

PARQUET_DIR = "/tmp/splp-logs"

TIMESTAMP_RE = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\]")
INT_RE = re.compile(r"[-+]?\d+")

PARQUET_SCHEMA = pa.schema(
    [
        ("apiName", pa.string()),
        ("proxyResponseCode", pa.int64()),
        ("errorType", pa.string()),
        ("destination", pa.string()),
        ("apiCreatorTenantDomain", pa.string()),
        ("platform", pa.string()),
        ("apiMethod", pa.string()),
        ("apiVersion", pa.string()),
        ("gatewayType", pa.string()),
        ("apiCreator", pa.string()),
        ("responseCacheHit", pa.bool_()),
        ("backendLatency", pa.int64()),
        ("correlationId", pa.string()),
        ("requestMediationLatency", pa.int64()),
        ("keyType", pa.string()),
        ("apiId", pa.string()),
        ("applicationName", pa.string()),
        ("targetResponseCode", pa.int64()),
        ("requestTimestamp", pa.timestamp("us")),
        ("applicationOwner", pa.string()),
        ("userAgent", pa.string()),
        ("eventType", pa.string()),
        ("apiResourceTemplate", pa.string()),
        ("regionId", pa.string()),
        ("responseLatency", pa.int64()),
        ("responseMediationLatency", pa.int64()),
        ("userIp", pa.string()),
        ("apiContext", pa.string()),
        ("applicationId", pa.string()),
        ("apiType", pa.string()),
        ("stream", pa.string()),
        ("time", pa.timestamp("us")),
        ("logTime", pa.timestamp("us")),
    ]
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_CONN_ID = "SPLP_minio"
MINIO_NDJSON_BUCKET = "splp-logs-raw"
MINIO_PARQUET_BUCKET = "splp-logs"
MINIO_PREFIX = "year={year}/month={month}/day={day}"
TMP_LOCAL_DIR = "/tmp/splp-logs"


def _parse_datetime(value: str | None) -> Optional[datetime]:
    if not value:
        return None

    normalized = value.replace(",", ".")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    if "." in normalized:
        prefix, frac = normalized.split(".", 1)
        frac_digits = "".join(ch for ch in frac if ch.isdigit())
        tz_suffix = frac[len(frac_digits) :]
        frac_digits = (frac_digits + "000000")[:6]
        normalized = f"{prefix}.{frac_digits}{tz_suffix}"

    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


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


def _build_minio_ndjson_key(day: datetime) -> str:
    prefix = MINIO_PREFIX.format(year=day.year, month=day.month, day=day.day)
    return f"{prefix}/logs.ndjson"


def _build_local_ndjson_path(day: datetime) -> str:
    return os.path.join(
        TMP_LOCAL_DIR,
        f"year={day.year}",
        f"month={day.month}",
        f"day={day.day}",
        "logs.ndjson",
    )


def _build_parquet_path(base_dir: str, day: datetime) -> str:
    return os.path.join(
        base_dir,
        f"year={day.year}",
        f"month={day.month}",
        f"day={day.day}",
        "logs.parquet",
    )


def _build_minio_parquet_key(day: datetime) -> str:
    prefix = MINIO_PREFIX.format(year=day.year, month=day.month, day=day.day)
    return f"{prefix}/logs.parquet"


def _upload_parquet_to_minio(local_path: str, day: datetime) -> None:
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    key = _build_minio_parquet_key(day)
    s3.load_file(
        filename=local_path,
        key=key,
        bucket_name=MINIO_PARQUET_BUCKET,
        replace=True,
    )


def _extract_metric_block(log_message: str) -> Optional[str]:
    marker = "Metric Value:"
    marker_idx = log_message.find(marker)
    if marker_idx == -1:
        return None

    start = log_message.find("{", marker_idx)
    if start == -1:
        return None

    brace_level = 0
    end = None
    for i, ch in enumerate(log_message[start:], start=start):
        if ch == "{":
            brace_level += 1
        elif ch == "}":
            brace_level -= 1
            if brace_level == 0:
                end = i
                break
    if end is None or end <= start:
        return None

    return log_message[start + 1 : end]


def _split_metric_pairs(metric_value_str: str) -> Iterable[Tuple[str, str]]:
    current: List[str] = []
    paren_level = 0
    brace_level = 0

    for ch in metric_value_str:
        if ch == "(":
            paren_level += 1
        elif ch == ")":
            paren_level = max(paren_level - 1, 0)
        elif ch == "{":
            brace_level += 1
        elif ch == "}":
            brace_level = max(brace_level - 1, 0)

        if ch == "," and paren_level == 0 and brace_level == 0:
            token = "".join(current).strip()
            if token and "=" in token:
                key, value = token.split("=", 1)
                yield key.strip(), value.strip()
            current = []
        else:
            current.append(ch)

    token = "".join(current).strip()
    if token and "=" in token:
        key, value = token.split("=", 1)
        yield key.strip(), value.strip()


def _cast_metric_value(key: str, value: str | None) -> Any:
    if value is None:
        return None

    value = value.strip()
    if value.lower() in {"null", "none", "nan", ""}:
        return None

    if key in {
        "proxyResponseCode",
        "backendLatency",
        "requestMediationLatency",
        "targetResponseCode",
        "responseLatency",
        "responseMediationLatency",
    }:
        return int(value) if INT_RE.fullmatch(value) else None

    if key == "responseCacheHit":
        return value.lower() == "true"

    if key == "requestTimestamp":
        return _parse_datetime(value)

    return value


def _parse_log_record(log_content: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    log_message = log_content.get("log", "")
    if not log_message:
        logger.warning("Skipping record with empty log message")
        return None

    match = TIMESTAMP_RE.search(log_message)
    log_time = _parse_datetime(match.group(1)) if match else None

    metric_block = _extract_metric_block(log_message)
    if not metric_block:
        logger.warning("Skipping record with missing Metric Value block")
        return None

    metric_values: Dict[str, Any] = {}
    for key, value in _split_metric_pairs(metric_block):
        metric_values[key] = _cast_metric_value(key, value)

    record = {
        "apiName": None,
        "proxyResponseCode": None,
        "errorType": None,
        "destination": None,
        "apiCreatorTenantDomain": None,
        "platform": None,
        "apiMethod": None,
        "apiVersion": None,
        "gatewayType": None,
        "apiCreator": None,
        "responseCacheHit": None,
        "backendLatency": None,
        "correlationId": None,
        "requestMediationLatency": None,
        "keyType": None,
        "apiId": None,
        "applicationName": None,
        "targetResponseCode": None,
        "requestTimestamp": None,
        "applicationOwner": None,
        "userAgent": None,
        "eventType": None,
        "apiResourceTemplate": None,
        "regionId": None,
        "responseLatency": None,
        "responseMediationLatency": None,
        "userIp": None,
        "apiContext": None,
        "applicationId": None,
        "apiType": None,
        "stream": str(log_content.get("stream", "")),
        "time": _parse_datetime(log_content.get("time")),
        "logTime": log_time,
    }

    record.update(metric_values)

    return record


def _iter_ndjson_batches(file_path: str, batch_size: int = BATCH_SIZE) -> Iterable[List[Dict[str, Any]]]:
    batch: List[Dict[str, Any]] = []
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if not line:
                continue
            try:
                batch.append(json.loads(line))
            except json.JSONDecodeError as exc:
                logger.exception("Skipping invalid JSON line: %s", exc)
                continue
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def _download_ndjson_from_minio(day: datetime, local_path: str) -> None:
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    key = _build_minio_ndjson_key(day)
    local_dir = os.path.dirname(local_path)
    os.makedirs(local_dir, exist_ok=True)
    tmp_path = s3.download_file(
        key=key,
        bucket_name=MINIO_NDJSON_BUCKET,
        local_path=local_dir,
    )
    os.replace(tmp_path, local_path)


def convert_ndjson_to_parquet(
    logical_date: Optional[datetime] = None,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
) -> Dict[str, Any]:
    start_dt, _ = resolve_window(
        logical_date=logical_date,
        window_start=window_start,
        window_end=window_end,
        default_lookback_days=DEFAULT_LOOKBACK_DAYS,
    )
    ndjson_path = _build_local_ndjson_path(start_dt)
    parquet_path = _build_parquet_path(PARQUET_DIR, start_dt)
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    _download_ndjson_from_minio(start_dt, ndjson_path)

    writer: Optional[pq.ParquetWriter] = None
    total_records = 0
    invalid_records = 0
    min_time: Optional[datetime] = None
    max_time: Optional[datetime] = None

    for batch in _iter_ndjson_batches(ndjson_path):
        parsed_records: List[Dict[str, Any]] = []
        for raw in batch:
            parsed = _parse_log_record(raw)
            if parsed:
                parsed_records.append(parsed)
            else:
                invalid_records += 1

        if not parsed_records:
            continue

        for record in parsed_records:
            record_time = record.get("time")
            if record_time and (min_time is None or record_time < min_time):
                min_time = record_time
            if record_time and (max_time is None or record_time > max_time):
                max_time = record_time

        table = pa.Table.from_pylist(parsed_records, schema=PARQUET_SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, PARQUET_SCHEMA, compression="snappy")
        writer.write_table(table)
        total_records += len(parsed_records)

    if writer is not None:
        writer.close()

    _upload_parquet_to_minio(parquet_path, start_dt)

    print(
        "Parquet conversion complete: "
        f"date={start_dt.strftime('%Y-%m-%d')}, "
        f"total_records={total_records}, "
        f"invalid_records={invalid_records}, "
        f"min_time={min_time.isoformat() if min_time else None}, "
        f"max_time={max_time.isoformat() if max_time else None}, "
        f"parquet_minio_key={_build_minio_parquet_key(start_dt)}"
    )

    return None
