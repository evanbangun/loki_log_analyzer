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

tenantDomain = set()
config = yaml.safe_load(open("config.yaml"))

def parse_log_content(log_content):
    """Parse the structured log content into a dictionary of fields."""
    try:
        log_message = log_content["log"]
        
        metric_match = re.search(r'Metric Value: {(.*?)}', log_message)
        if metric_match:
            metric_value_str = metric_match.group(1)
            
            metric_values = {}
            for pair in metric_value_str.split(', '):
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    if key in ['proxyResponseCode', 'backendLatency', 'requestMediationLatency', 
                             'targetResponseCode', 'responseLatency', 'responseMediationLatency']:
                        value = int(value) if value.isdigit() else None
                    elif key in ['responseCacheHit']:
                        value = True if value.lower() == 'true' else False
                    elif key in ['requestTimestamp']:
                        value = pd.to_datetime(value).tz_localize(None) if value else None
                    else:
                        value = str(value) if value else None
                    metric_values[key] = value
            
            default_values = {
                'apiName': None,
                'proxyResponseCode': None,
                'errorType': None,
                'destination': None,
                'apiCreatorTenantDomain': None,
                'platform': None,
                'apiMethod': None,
                'apiVersion': None,
                'gatewayType': None,
                'apiCreator': None,
                'responseCacheHit': None,
                'backendLatency': None,
                'correlationId': None,
                'requestMediationLatency': None,
                'keyType': None,
                'apiId': None,
                'applicationName': None,
                'targetResponseCode': None,
                'requestTimestamp': None,
                'applicationOwner': None,
                'userAgent': None,
                'eventType': None,
                'apiResourceTemplate': None,
                'regionId': None,
                'responseLatency': None,
                'responseMediationLatency': None,
                'userIp': None,
                'apiContext': None,
                'applicationId': None,
                'apiType': None,
                'stream': str(log_content.get("stream")) if log_content.get("stream") else None,
                'time': pd.to_datetime(log_content.get("time")).tz_localize(None) if log_content.get("time") else None
            }
            
            default_values.update(metric_values)
            
            return default_values
            
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

    timestamp_columns = ['requestTimestamp', 'time']
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.floor('us')
    
    df = df.sort_values("time", kind="stable")
    
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

def get_date_range():
    try:
        choice = input("1. All Dates \n2. Single Date \n3. Date Range \nTime Range : ")
        if choice == "1":
            return None, None
        elif choice == "2":
            while True:
                try:
                    date_str = input("Enter date (YYYY-MM-DD): ")
                    date = pd.to_datetime(date_str)
                    return date, date
                except ValueError:
                    print("Invalid date format. Please use YYYY-MM-DD format.")
                    sys.exit()
        elif choice == "3":
            while True:
                try:
                    date_range = input("Enter date range (YYYY-MM-DD//YYYY-MM-DD): ")
                    start_date, end_date = date_range.split("//")
                    if start_date > end_date:
                        print("Start date must be before end date. Please try again.")
                        continue
                    return pd.to_datetime(start_date), pd.to_datetime(end_date)
                except ValueError:
                    print("Invalid date format. Please use YYYY-MM-DD format.")
                    sys.exit()
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")
            sys.exit()
    except Exception as e:
        print(f"Error: {str(e)}")

def convert_ndjson_to_parquet():
    ndjson_dir = "logs"
    parquet_dir = "D:/SPLP_Logs_parquet"
    
    start_date, end_date = get_date_range()
    
    os.makedirs(parquet_dir, exist_ok=True)
    
    ndjson_files = [f for f in os.listdir(ndjson_dir) if f.endswith('.txt')]
    total_records = 0
    
    files_to_process = []
    for file_name in ndjson_files:
        try:
            date_str = file_name.replace('logs_', '').replace('.txt', '')
            file_date = pd.to_datetime(date_str)
            
            if start_date is None or (start_date <= file_date <= end_date):
                files_to_process.append(file_name)
        except ValueError:
            print(f"Skipping file with invalid date format: {file_name}")
            continue
    
    if not files_to_process:
        if start_date is None:
            print("No files found in the directory")
        elif start_date == end_date:
            print(f"No files found for date {start_date.strftime('%Y-%m-%d')}")
        else:
            print(f"No files found in the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return 0
    
    if start_date is None:
        print(f"\nProcessing all {len(files_to_process)} files")
    elif start_date == end_date:
        print(f"\nProcessing files for date {start_date.strftime('%Y-%m-%d')}")
    else:
        print(f"\nProcessing files from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    for file_name in files_to_process:
        date_str = file_name.replace('logs_', '').replace('.txt', '')
        current_date = pd.to_datetime(date_str)
        
        file_path = os.path.join(ndjson_dir, file_name)
        print(f"\nProcessing {file_name}...")
        
        current_logs = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    log_content = json.loads(line.strip())
                    current_logs.append(log_content)
                except json.JSONDecodeError as e:
                    print(f"Error parsing line in {file_name}: {str(e)}")
                    continue
        
        if current_logs:
            write_logs_to_parquet(current_logs, current_date, parquet_dir)
            total_records += len(current_logs)
            print(f"Converted {len(current_logs)} records from {file_name}")
    
    return total_records

if __name__ == "__main__":
    total = convert_ndjson_to_parquet()
    print(f"\nConversion complete. Total records converted: {total}")
