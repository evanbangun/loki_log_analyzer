import requests
import json
import re
from datetime import datetime, timedelta
import time
import pandas as pd

tenantDomain = set()

def get_logs(start_date, end_date):
    # Loki API endpoint
    url = "http://localhost:3100/loki/api/v1/query_range"

    pattern = re.compile(r'apiCreatorTenantDomain=([^,]+)')

    current_date = start_date

    cur_end_date = end_date

    total_record = 0

    while current_date < end_date:
        # Query parameters
<<<<<<< HEAD

        if pd.Timestamp(current_date) + pd.Timedelta(hours=1) > pd.Timestamp(end_date):
            cur_end_date = end_date
        else:
            cur_end_date = (pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z')

        print("check current date and end date : ", current_date, cur_end_date)
        params = {
            "query": '{container="splp-gw"} |~ "Metric Name: apim:response"',
            "start": current_date,
            "end": cur_end_date,
            "limit": 5000,
            "direction" : "FORWARD"
        }

        # Make the request
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            logs_count = 0

            # Print each log entry
            for stream in data['data']['result']:
                logs_count += len(stream['values'])
                for value in stream['values']:
                    log_content = json.loads(value[1])
                    if current_date < log_content["time"]:
                        current_date = log_content["time"]
                    match = pattern.search(log_content["log"])
                    if match:
                        tenantDomain.add(match.group(1))
            total_record += logs_count
            if logs_count < 5000 and cur_end_date == end_date:
                print("last iteration : ", current_date, cur_end_date, logs_count)
                # Print the full URL with parameters
                full_url = f"{url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
                print(f"Query URL: {full_url}\n")
                break
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            print("last iteration : ", current_date, cur_end_date, logs_count)
            # Print the full URL with parameters
            full_url = f"{url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
            print(f"Query URL: {full_url}\n")
            break

        time.sleep(0.05)

=======

        if pd.Timestamp(current_date) + pd.Timedelta(hours=1) > pd.Timestamp(end_date):
            # print("check current date and end date : ", current_date, end_date)
            # print("current_date : " + str(pd.Timestamp(current_date)))
            # print("end_date : " + str(pd.Timestamp(end_date)))
            params = {
                "query": '{container="splp-gw"} |~ "Metric Name: apim:response"',
                "start": current_date,
                "end": end_date,
                "limit": 5000,
                "direction" : "FORWARD"
            }
        else:
            # print("check current date and end date : ", current_date, (pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z'))
            # print("current_date : " + str(pd.Timestamp(current_date)))
            # print("end_date : " + str((pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z'))
            params = {
                "query": '{container="splp-gw"} |~ "Metric Name: apim:response"',
                "start": current_date,
                "end": (pd.Timestamp(current_date) + pd.Timedelta(hours=1)).isoformat().replace('+00:00', 'Z'),
                "limit": 5000,
                "direction" : "FORWARD"
            }

        # Make the request
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            logs_count = 0

            # Print each log entry
            for stream in data['data']['result']:
                logs_count += len(stream['values'])
                for value in stream['values']:
                    log_content = json.loads(value[1])
                    if current_date < log_content["time"]:
                        current_date = log_content["time"]
                        # print("update current date", current_date)
                    match = pattern.search(log_content["log"])
                    if match:
                        tenantDomain.add(match.group(1))
            total_record += logs_count
            if logs_count < 5000:
                # print("last iteration : ", current_date, end_date, logs_count)
                break
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
    
        time.sleep(0.05)
>>>>>>> origin/main


    return str(total_record)

if __name__ == "__main__":
    start_date = '2025-01-01T00:00:00.000000000Z'
<<<<<<< HEAD
    end_date = '2025-01-31T23:59:59.999999999Z'
=======
    end_date = '2025-01-01T01:59:59.999999999Z'
>>>>>>> origin/main
    # print(pd.Timestamp(start_date))
    # print(pd.Timestamp(end_date))
    # print((pd.Timestamp(start_date)).isoformat().replace('+00:00', 'Z'))
    # print((pd.Timestamp(end_date)).isoformat().replace('+00:00', 'Z'))
    print("Total records January = " + get_logs(start_date, end_date))
    
    # start_date = '2025-02-01T00:00:00.000000000Z'
    # end_date = '2025-02-31T23:59:59.999999999Z'
    # print("Total records February = " + get_logs(start_date, end_date))

    # start_date = '2025-03-01T00:00:00.000000000Z'
    # end_date = '2025-03-31T23:59:59.999999999Z'
    # print("Total records March = " + get_logs(start_date, end_date))
    
    with open('extracted_logs.txt', 'a', encoding='utf-8', buffering=1024*1024) as outfile:
        for element in tenantDomain:
            outfile.write(str(element) + '\n')