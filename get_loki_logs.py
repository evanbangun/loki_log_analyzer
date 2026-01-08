import requests
import json
import re
from datetime import datetime, timedelta
import time
import pandas as pd
import os
import yaml
from typing import List, Dict, Optional, Tuple, Any
import pyarrow as pa
import pyarrow.parquet as pq
import sys

config = yaml.safe_load(open("config.yaml"))
url = config['CONFIG']["LOKI_URL"]
limit = int(config['CONFIG']['LIMIT'])

def get_logs_ndjson(start_date, end_date):
    log_dir = config['CONFIG']['LOG_DIR_NDJSON']
    current_date = start_date
    cur_end_date = end_date
    total_record = 0
    os.makedirs(log_dir, exist_ok=True)

    print_time = current_date
    print_time_str = pd.Timestamp(print_time).strftime('%Y-%m-%d')
    extract_path = os.path.join(log_dir, f'logs_{print_time_str}.txt')
    outfile = open(extract_path, 'w', encoding='utf-8', buffering=1024*1024)
    
    print("iterating through : ", (pd.Timestamp(current_date)).date())

    while current_date < end_date:
        print(current_date)
        if (pd.Timestamp(print_time)).day != (pd.Timestamp(current_date)).day:
            outfile.close()
            print_time = current_date
            print_time_str = pd.Timestamp(print_time).strftime('%Y-%m-%d')
            extract_path = os.path.join(log_dir, f'logs_{print_time_str}.txt')
            outfile = open(extract_path, 'w', encoding='utf-8', buffering=1024*1024)
            print("iterating through : ", (pd.Timestamp(current_date)).date())
        
        if pd.Timestamp(current_date) + pd.Timedelta(hours=1) > pd.Timestamp(end_date):
            cur_end_date = end_date
        else:
            cur_end_date = (pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')

        params = {
            'query': config['CONFIG']['QUERY'],
            "start": current_date,
            "end": cur_end_date,
            "limit": limit,
            "direction" : "FORWARD"
        }

        new_logs_found = False

        response = requests.get(url, params=params, timeout=(10, 600))

        if response.status_code == 200:
            data = response.json()
            logs_count = 0
            for stream in data['data']['result']:
                logs_count += len(stream['values'])
                for value in stream['values']:
                    log_content = json.loads(value[1])
                    if current_date < log_content["time"]:
                        current_date = log_content["time"]
                        new_logs_found = True
                    outfile.write(json.dumps(log_content) + '\n')
            
            total_record += logs_count
            if logs_count < limit and cur_end_date == end_date:
                print("last iteration : ", current_date, cur_end_date, logs_count)
                break
            if not new_logs_found:
                current_date = cur_end_date
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            current_date = cur_end_date
            cur_end_date = (pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
            print("iteration error on date range : ", current_date, cur_end_date)

    outfile.close()
    return str(total_record)

def get_logs_parquet(start_date, end_date):
    try:
        log_dir = config['CONFIG']['LOG_DIR_PARQUET']
        total_record = 0
        current_date = start_date
        cur_end_date = end_date
                
        schema = pa.schema([
                    ('apiName', pa.string()),
                    ('proxyResponseCode', pa.int64()),
                    ('destination', pa.string()),
                    ('apiCreatorTenantDomain', pa.string()),
                    ('platform', pa.string()),
                    ('apiMethod', pa.string()),
                    ('apiVersion', pa.string()),
                    ('gatewayType', pa.string()),
                    ('apiCreator', pa.string()),
                    ('responseCacheHit', pa.bool_()),
                    ('backendLatency', pa.int64()),
                    ('correlationId', pa.string()),
                    ('requestMediationLatency', pa.int64()),
                    ('keyType', pa.string()),
                    ('apiId', pa.string()),
                    ('applicationName', pa.string()),
                    ('targetResponseCode', pa.int64()),
                    ('requestTimestamp', pa.timestamp('us')),
                    ('applicationOwner', pa.string()),
                    ('userAgent', pa.string()),
                    ('eventType', pa.string()),
                    ('apiResourceTemplate', pa.string()),
                    ('regionId', pa.string()),
                    ('responseLatency', pa.int64()),
                    ('responseMediationLatency', pa.int64()),
                    ('userIp', pa.string()),
                    ('apiContext', pa.string()),
                    ('applicationId', pa.string()),
                    ('apiType', pa.string()),
                    ('stream', pa.string()),
                    ('time', pa.timestamp('us')),
                    ('logTime', pa.timestamp('us'))  # Added Log Time column to schema
                ])
            
        os.makedirs(log_dir, exist_ok=True)

        print_time = current_date
        
        print("iterating through : ", (pd.Timestamp(current_date)).date())
        
        writer = None
        record_count = 0
        while current_date < end_date:
            print(current_date)
            if (pd.Timestamp(print_time)).day != (pd.Timestamp(current_date)).day:
                if writer:
                    writer.close()
                print_time = current_date
                dt = pd.Timestamp(current_date)
                path = os.path.join(
                    log_dir,
                    f"year={dt.year}/month={dt.month:02}/day={dt.day:02}/logs.parquet"
                )
                writer = pq.ParquetWriter(path, schema=schema, compression='snappy')
                print("iterating through : ", (pd.Timestamp(current_date)).date())
            
            if pd.Timestamp(current_date) + pd.Timedelta(hours=1) > pd.Timestamp(end_date):
                cur_end_date = end_date
            else:
                cur_end_date = (pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')

            params = {
                'query': config['CONFIG']['QUERY'],
                "start": current_date,
                "end": cur_end_date,
                "limit": limit,
                "direction" : "FORWARD"
            }

            new_logs_found = False

            response = requests.get(url, params=params, timeout=(10, 600))
            if response.status_code == 200:
                data = response.json()
                logs_count = 0
                for stream in data['data']['result']:
                    logs_count += len(stream['values'])
                    for value in stream['values']:
                        log_content = json.loads(value[1])
                        if current_date < log_content["time"]:
                            current_date = log_content["time"]
                            new_logs_found = True
                        parsed_log = parse_log_content(log_content["log"], log_content["time"], log_content["stream"])
                        batch = pa.Table.from_pylist([parsed_log], schema=schema)
                        writer.write_table(batch)
                        record_count += 1
            
                total_record += logs_count
                if logs_count < limit and cur_end_date == end_date:
                    print("last iteration : ", current_date, cur_end_date, logs_count)
                    break
                if not new_logs_found:
                    current_date = cur_end_date

        print(f"ETL complete. {record_count}/{total_record} files processed.")
    except Exception as e:
        print(f"Error: {e}")
        print(e)
        current_date = cur_end_date
        cur_end_date = (pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
        print("iteration error on date range : ", current_date, cur_end_date)

    return str(total_record)

def parse_log_content(log, time, stream):
    try:
        log_message = log
        logTime = None
        TIMESTAMP_RE = re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\]')
        match = TIMESTAMP_RE.search(log)
        if match:
            timestamp_str = match.group(1)
            # Convert comma to dot for milliseconds (e.g., "00:19:26,251" -> "00:19:26.251")
            timestamp_str = timestamp_str.replace(',', '.')
            logTime = pd.to_datetime(timestamp_str).tz_localize(None)
        else:
            print("No timestamp found in log message")
            logTime = None
        
        marker = 'Metric Value:'
        marker_idx = log_message.find(marker)
        if marker_idx == -1:
            return None
        # Find the first opening brace after the marker
        start = log_message.find('{', marker_idx)
        if start == -1:
            return None
        brace_level = 0
        end = None
        for i, ch in enumerate(log_message[start:], start=start):
            if ch == '{':
                brace_level += 1
            elif ch == '}':
                brace_level -= 1
                if brace_level == 0:
                    end = i
                    break
        if end is None or end <= start:
            return None
        log_message = log_message[start + 1:end]

        metric_values: Dict[str, Any] = {}
        current: List[str] = []
        paren_level = 0
        brace_level = 0
        INT_RE = re.compile(r"[-+]?\d+")
        for ch in log_message:
            if ch == '(':
                paren_level += 1
            elif ch == ')':
                paren_level = max(paren_level - 1, 0)
            elif ch == '{':
                brace_level += 1
            elif ch == '}':
                brace_level = max(brace_level - 1, 0)
            if ch == ',' and paren_level == 0 and brace_level == 0:
                token = ''.join(current).strip()
                if token:
                    key, value = token.split('=', 1)
                    if key is not None:
                        if value is None or (value.strip().lower() in {"null", "none", "nan", ""}):
                            metric_values[key] = None
                        if key in ['proxyResponseCode', 'backendLatency', 'requestMediationLatency', 
                                'targetResponseCode', 'responseLatency', 'responseMediationLatency']:
                            value = value.strip()
                            metric_values[key] = int(value) if INT_RE.fullmatch(value) else None
                        elif key in ['responseCacheHit']:
                            metric_values[key] = value.lower() == 'true'
                        elif key in ['requestTimestamp']:
                            metric_values[key] = pd.to_datetime(value).tz_localize(None) if value else None
                        else:
                            metric_values[key] = str(value) if value else None
                current = []
            else:
                current.append(ch)

        token = ''.join(current).strip()
        if token:
            key, value = token.split('=', 1)
            if key is not None:
                if value is None or (value.strip().lower() in {"null", "none", "nan", ""}):
                    metric_values[key] = None
                if key in ['proxyResponseCode', 'backendLatency', 'requestMediationLatency', 
                        'targetResponseCode', 'responseLatency', 'responseMediationLatency']:
                    value = value.strip()
                    metric_values[key] = int(value) if INT_RE.fullmatch(value) else None
                elif key in ['responseCacheHit']:
                    metric_values[key] = value.lower() == 'true'
                elif key in ['requestTimestamp']:
                    metric_values[key] = pd.to_datetime(value).tz_localize(None) if value else None
                else:
                    metric_values[key] = str(value) if value else None

        default_values = {
            'apiName': None, 'proxyResponseCode': None, 'errorType': None,
            'destination': None, 'apiCreatorTenantDomain': None, 'platform': None,
            'apiMethod': None, 'apiVersion': None, 'gatewayType': None,
            'apiCreator': None, 'responseCacheHit': None, 'backendLatency': None,
            'correlationId': None, 'requestMediationLatency': None, 'keyType': None,
            'apiId': None, 'applicationName': None, 'targetResponseCode': None,
            'requestTimestamp': None, 'applicationOwner': None, 'userAgent': None,
            'eventType': None, 'apiResourceTemplate': None, 'regionId': None,
            'responseLatency': None, 'responseMediationLatency': None, 'userIp': None,
            'apiContext': None, 'applicationId': None, 'apiType': None,
            'stream': stream,
            'time': pd.to_datetime(time).tz_localize(None) if time else None,
            'logTime': logTime # Extracted from log content
        }

        default_values.update(metric_values)

        return default_values
    except Exception as e:
        print(f"Error parsing log content: {str(e)}")
        print(e)
        return None
    

if __name__ == "__main__":
    start_date = config['CONFIG']['START_DATE']
    end_date = config['CONFIG']['END_DATE']
    format_choice = input("1. NDJSON (.txt)\n2. Parquet (.parquet)\nEnter your choice (1/2): ")
    
    if format_choice == "1":
        print("Total records = " + get_logs_ndjson(start_date, end_date))
    elif format_choice == "2":
        print("Total records = " + get_logs_parquet(start_date, end_date))
    else:
        print("Invalid choice.")
        sys.exit()