import requests
import json
import re
from datetime import datetime, timedelta
import time
import pandas as pd
import os
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import sys

config = yaml.safe_load(open("config.yaml"))

def parse_log_content(log_content):
    try:
        log_message = log_content["log"]
        
        metric_match = re.search(r'Metric Value: {(.*?)}', log_message)
        if metric_match:
            metric_value_str = metric_match.group(1)
            
            metric_values = {}
            for pair in metric_value_str.split(', '):
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    if value.lower() == 'true':
                        value = True
                    elif value.lower() == 'false':
                        value = False
                    elif value.isdigit():
                        value = int(value)
                    metric_values[key] = value
            
            return {
                **metric_values,
                "stream": log_content.get("stream"),
                "time": pd.to_datetime(log_content.get("time"))
            }
    except Exception as e:
        print(f"Error parsing log content: {str(e)}")
        return None

def write_logs_to_parquet(logs, current_date, log_dir):
    parsed_logs = []
    for log in logs:
        parsed_log = parse_log_content(log)
        if parsed_log:
            parsed_logs.append(parsed_log)
    
    if not parsed_logs:
        print("No valid logs to write")
        return
        
    df = pd.DataFrame(parsed_logs)
    
    schema = pa.schema([
        ('apiName', pa.string()),
        ('proxyResponseCode', pa.int32()), 
        ('destination', pa.string()),
        ('apiCreatorTenantDomain', pa.string()),
        ('platform', pa.string()),
        ('apiMethod', pa.string()),
        ('apiVersion', pa.string()),
        ('gatewayType', pa.string()),
        ('apiCreator', pa.string()),
        ('responseCacheHit', pa.bool_()),
        ('backendLatency', pa.int32()), 
        ('correlationId', pa.string()),
        ('requestMediationLatency', pa.int32()),
        ('keyType', pa.string()),
        ('apiId', pa.string()),
        ('applicationName', pa.string()),
        ('targetResponseCode', pa.int32()),
        ('requestTimestamp', pa.timestamp('us')), 
        ('applicationOwner', pa.string()),
        ('userAgent', pa.string()),
        ('eventType', pa.string()),
        ('apiResourceTemplate', pa.string()),
        ('regionId', pa.string()),
        ('responseLatency', pa.int32()), 
        ('responseMediationLatency', pa.int32()),
        ('userIp', pa.string()),
        ('apiContext', pa.string()),
        ('applicationId', pa.string()),
        ('apiType', pa.string()),
        ('stream', pa.string()),
        ('time', pa.timestamp('us'))  
    ])
    
    table = pa.Table.from_pandas(df, schema=schema)
    
    print_time_str = pd.Timestamp(current_date).strftime('%Y-%m-%d')
    partition_path = os.path.join(log_dir, f'day={print_time_str}')
    os.makedirs(partition_path, exist_ok=True)
    
    parquet_path = os.path.join(partition_path, 'logs.parquet')
    pq.write_table(table, parquet_path, compression='snappy')
    print(f"Written {len(parsed_logs)} records to {parquet_path}")


def get_logs_parquet(start_date, end_date):
    url = config['CONFIG']["LOKI_URL"]
    current_date = start_date
    cur_end_date = end_date
    total_record = 0
    limit = int(config['CONFIG']['LIMIT'])
    log_dir = config['CONFIG']['LOG_DIR_PARQUET']
    
    os.makedirs(log_dir, exist_ok=True)

    current_logs = []
    
    print("iterating through : ", (pd.Timestamp(current_date)).date())

    while current_date < end_date:
        if (pd.Timestamp(current_date)).day != (pd.Timestamp(current_date + pd.Timedelta(hours=1))).day:
            if current_logs:
                write_logs_to_parquet(current_logs, current_date, log_dir)
                current_logs = []
            print("iterating through : ", (pd.Timestamp(current_date)).date())
        
        if pd.Timestamp(current_date) + pd.Timedelta(hours=1) > pd.Timestamp(end_date):
            cur_end_date = end_date
        else:
            cur_end_date = (pd.Timestamp(cur_end_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')

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
                    current_logs.append(log_content)
            
            total_record += logs_count
            if logs_count < limit and cur_end_date == end_date:
                print("last iteration : ", current_date, cur_end_date, logs_count)
                break
            if not new_logs_found:
                current_date = cur_end_date
                cur_end_date = (pd.Timestamp(cur_end_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            current_date = cur_end_date
            cur_end_date = (pd.Timestamp(cur_end_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
            print("iteration error on date range : ", current_date, cur_end_date)

    if current_logs:
        write_logs_to_parquet(current_logs, current_date, log_dir)

    return str(total_record)

def get_logs_ndjson(start_date, end_date):
    url = config['CONFIG']["LOKI_URL"]
    current_date = start_date
    cur_end_date = end_date
    total_record = 0
    limit = int(config['CONFIG']['LIMIT'])
    log_dir = config['CONFIG']['LOG_DIR_NDJSON']
    
    os.makedirs(log_dir, exist_ok=True)

    print_time = current_date
    print_time_str = pd.Timestamp(print_time).strftime('%Y-%m-%d')
    extract_path = os.path.join(log_dir, f'logs_{print_time_str}.txt')
    outfile = open(extract_path, 'w', encoding='utf-8', buffering=1024*1024)
    
    print("iterating through : ", (pd.Timestamp(current_date)).date())

    while current_date < end_date:
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
            cur_end_date = (pd.Timestamp(cur_end_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')

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
                cur_end_date = (pd.Timestamp(cur_end_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            current_date = cur_end_date
            cur_end_date = (pd.Timestamp(cur_end_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')
            print("iteration error on date range : ", current_date, cur_end_date)

    outfile.close()
    return str(total_record)


if __name__ == "__main__":
    start_date = config['CONFIG']['START_DATE']
    end_date = config['CONFIG']['END_DATE']
    format_choice = input("1. NDJSON (.txt)\n2. Parquet file (.parquet)\nEnter your choice (1/2): ")
    
    if format_choice == "1":
        print("Total records = " + get_logs_ndjson(start_date, end_date))
    elif format_choice == "2":
        print("Total records = " + get_logs_parquet(start_date, end_date))
    else:
        print("Invalid choice.")
        sys.exit()