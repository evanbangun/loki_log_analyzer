import requests
import json
import re
from datetime import datetime, timedelta
import time
import pandas as pd
import os
import yaml

tenantDomain = set()
config = yaml.safe_load(open("config.yaml"))

def get_logs(start_date, end_date):
    url = config['CONFIG']["LOKI_URL"]

    current_date = start_date
    cur_end_date = end_date

    total_record = 0

    limit = int(config['CONFIG']['LIMIT'])

    log_dir = config['CONFIG']['LOG_DIR']
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

    print("Total records = " + get_logs(start_date, end_date))