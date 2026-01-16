import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml


PARQUET_SCHEMA = pa.schema(
    [
        ("apiName", pa.string()),
        ("proxyResponseCode", pa.int64()),
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

INT_RE = re.compile(r"[-+]?\d+")
TIMESTAMP_RE = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\]")
BATCH_SIZE = 1000


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def get_logs_parquet(config: Dict[str, Any], start_date: str, end_date: str) -> str:
    cfg = config.get("CONFIG", config)
    url = cfg.get("LOKI_URL")
    query = cfg.get("QUERY")
    limit = int(cfg.get("LIMIT", 5000))
    log_dir = cfg.get("LOG_DIR_PARQUET")

    if not url or not query or not start_date or not end_date or not log_dir:
        raise ValueError("Missing required configuration keys for Loki parquet extraction.")

    os.makedirs(log_dir, exist_ok=True)

    current_dt = _parse_datetime(start_date)
    end_dt = _parse_datetime(end_date)
    if not current_dt or not end_dt:
        raise ValueError("START_DATE and END_DATE must be valid timestamps.")

    cur_end_dt = end_dt
    total_record = 0

    record_count = 0
    writer = None
    active_day = None

    try:
        while current_dt < end_dt:
            print(_format_loki_time(current_dt))

            if current_dt + timedelta(hours=1) > end_dt:
                cur_end_dt = end_dt
            else:
                cur_end_dt = current_dt + timedelta(hours=1)

            params = {
                "query": query,
                "start": _format_loki_time(current_dt),
                "end": _format_loki_time(cur_end_dt),
                "limit": limit,
                "direction": "FORWARD",
            }

            new_logs_found = False

            response = requests.get(url, params=params, timeout=(10, 600))
            if response.status_code == 200:
                data = response.json()
                logs_count = 0
                for stream in data.get("data", {}).get("result", []):
                    logs_count += len(stream.get("values", []))
                    for value in stream.get("values", []):
                        log_content = json.loads(value[1])
                        log_time = _parse_datetime(log_content.get("time"))
                        if log_time and current_dt < log_time:
                            current_dt = log_time
                            new_logs_found = True

                        parsed_log = parse_log_content(
                            log_content.get("log"),
                            log_content.get("time"),
                            log_content.get("stream"),
                        )
                        if not parsed_log:
                            continue

                        event_time = parsed_log.get("time") or current_dt
                        dt = event_time if isinstance(event_time, datetime) else _parse_datetime(str(event_time))
                        if not dt:
                            continue
                        day_key = (dt.year, dt.month, dt.day)
                        if active_day != day_key:
                            if writer:
                                writer.close()
                            path = os.path.join(
                                log_dir,
                                f"year={dt.year}/month={dt.month:02}/day={dt.day:02}/logs.parquet",
                            )
                            os.makedirs(os.path.dirname(path), exist_ok=True)
                            writer = pq.ParquetWriter(path, PARQUET_SCHEMA, compression="snappy")
                            print("iterating through : ", dt.date())
                            active_day = day_key

                        batch = pa.Table.from_pylist([parsed_log], schema=PARQUET_SCHEMA)
                        writer.write_table(batch)
                        record_count += 1

                total_record += logs_count
                if logs_count < limit and cur_end_dt == end_dt:
                    print("last iteration : ", _format_loki_time(current_dt), _format_loki_time(cur_end_dt), logs_count)
                    break
                if not new_logs_found:
                    current_dt = cur_end_dt
            else:
                print(f"Error: {response.status_code}")
                print(response.text)
                current_dt = cur_end_dt
                cur_end_dt = current_dt + timedelta(hours=1)
                print(
                    "iteration error on date range : ",
                    _format_loki_time(current_dt),
                    _format_loki_time(cur_end_dt),
                )

        print(f"ETL complete. {record_count}/{total_record} files processed.")
    finally:
        if writer:
            writer.close()

    return str(total_record)


def parse_log_content(log: str | None, time: str | None, stream: str | None):
    if not log:
        return None

    try:
        log_message = log
        log_time = None
        match = TIMESTAMP_RE.search(log_message)
        if match:
            timestamp_str = match.group(1)
            log_time = _parse_datetime(timestamp_str)
        else:
            print("No timestamp found in log message")

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

        log_message = log_message[start + 1 : end]

        metric_values: Dict[str, Any] = {}
        current: List[str] = []
        paren_level = 0
        brace_level = 0
        for ch in log_message:
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
                if token:
                    key, value = token.split("=", 1)
                    metric_values[key] = _cast_metric_value(key, value)
                current = []
            else:
                current.append(ch)

        token = "".join(current).strip()
        if token:
            key, value = token.split("=", 1)
            metric_values[key] = _cast_metric_value(key, value)

        default_values = {
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
            "stream": stream,
            "time": _parse_datetime(time) if time else None,
            "logTime": log_time,
        }

        default_values.update(metric_values)
        return default_values
    except Exception as exc:
        print(f"Error parsing log content: {exc}")
        return None


def _cast_metric_value(key: str, value: str | None):
    if value is None:
        return None

    value = value.strip()
    if value.lower() in {"null", "none", "nan", ""}:
        return None

    if key in [
        "proxyResponseCode",
        "backendLatency",
        "requestMediationLatency",
        "targetResponseCode",
        "responseLatency",
        "responseMediationLatency",
    ]:
        return int(value) if INT_RE.fullmatch(value) else None

    if key in ["responseCacheHit"]:
        return value.lower() == "true"

    if key in ["requestTimestamp"]:
        return _parse_datetime(value) if value else None

    return str(value) if value else None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.replace(",", ".")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return pd.to_datetime(normalized).to_pydatetime().replace(tzinfo=None)
        except Exception:
            return None

    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _format_loki_time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def main() -> int:
    config = load_config("config.yaml")
    cfg = config.get("CONFIG", config)

    start_date = cfg.get("START_DATE")
    end_date = cfg.get("END_DATE")

    if not start_date or not end_date:
        raise ValueError("START_DATE and END_DATE must be defined in the configuration.")

    total = get_logs_parquet(config, start_date, end_date)
    print(f"Total records = {total}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Error running loki parquet ingestor: {exc}")
        raise
