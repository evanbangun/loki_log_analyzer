import requests
import json
import re
from datetime import datetime, timedelta
import time

tenantDomain = set()

def get_logs(start_date, end_date):
    # Loki API endpoint
    url = "http://localhost:3100/loki/api/v1/query_range"

    pattern = re.compile(r'apiCreatorTenantDomain=([^,]+)')

    current_date = start_date

    total_record = 0

    with open('extracted_logs.txt', 'w', encoding='utf-8', buffering=1024*1024) as outfile:
        while current_date < end_date:
            # Query parameters
            params = {
                "query": '{container="splp-gw"} |~ "Metric Name: apim:response"',
                "start": current_date,
                "end": end_date,
                "limit": 1,
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
                            # outfile.write(match.group(1) + '\n')
                total_record += logs_count
                if logs_count < 5000:
                    break
            else:
                print(f"Error: {response.status_code}")
                print(response.text)
        
            time.sleep(0.05)
        for element in tenantDomain:
            outfile.write(str(element) + '\n')

    # Print the full URL with parameters
    full_url = f"{url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
    print(f"Query URL: {full_url}\n")

    return str(total_record)

if __name__ == "__main__":
    start_date = '2025-02-01T00:00:00.000000000Z'
    end_date = '2025-02-01T00:00:00.999999999Z'
    print("Total records = " + get_logs(start_date, end_date))