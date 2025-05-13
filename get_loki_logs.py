import requests
import json
import re
from datetime import datetime, timedelta
import time
import pandas as pd
import os

tenantDomain = set()

def get_logs(start_date, end_date):
    # Loki API endpoint
    # url = "http://localhost:3100/loki/api/v1/query_range"
    url = "http://10.31.67.73:3100/loki/api/v1/query_range"

    pattern = re.compile(r'apiCreatorTenantDomain=([^,]+)')

    current_date = start_date

    cur_end_date = end_date

    total_record = 0

    limit = 5000

    log_dir = 'logs'
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

        # Query parameters
        params = {
            "query": '{container="splp-gw"} |~ "Metric Name: apim:response"',
            # "query": '{container="splp-gw"}',
            "start": current_date,
            "end": cur_end_date,
            "limit": limit,
            "direction" : "FORWARD"
        }

        new_logs_found = False
        
        # Print the full URL with parameters
        # full_url = f"{url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        # print(f"Query URL: {full_url}\n")

        # Make the request
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            logs_count = 0

            # print("current iteration : ", current_date, cur_end_date)

            for stream in data['data']['result']:
                logs_count += len(stream['values'])
                for value in stream['values']:
                    log_content = json.loads(value[1])
                    # print("current iteration : ", current_date, cur_end_date, logs_count, log_content["time"])
                    if current_date < log_content["time"]:
                        # print(current_date, log_content["time"])
                        current_date = log_content["time"]
                        new_logs_found = True
                    outfile.write(json.dumps(log_content) + '\n')
            #         match = pattern.search(log_content["log"])
            #         if match:
            #             tenantDomain.add(match.group(1))
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
        # time.sleep(0.05)
    outfile.close()

    return str(total_record)

if __name__ == "__main__":
    start_date = '2025-03-20T00:00:00Z'
    end_date = '2025-04-30T23:59:59.999999999Z'

    print("Total records = " + get_logs(start_date, end_date))