import json
import re
from collections import defaultdict
from pathlib import Path
import csv

def get_logs():
    tenantDomain = defaultdict(lambda: {"occurence": 0, "last_hit": None})
    # Loki API endpoint
    pattern_aCTD = re.compile(r'apiCreatorTenantDomain=([^,]+)')
    pattern_aI = re.compile(r'applicationId=([^,]+)')
    pattern_uI = re.compile(r'userIp=([^,]+)')

    folder = Path("logs")
    for file in folder.iterdir():
        if file.is_file():
            print("iterating through file : ", file.name)
            with file.open("r", encoding="utf-8") as f:
                for log_record in f:
                    if log_record.strip():
                        try:
                            log_content = json.loads(log_record)
                        except json.JSONDecodeError:
                            print(file.name, log_record)
                            continue
                        match_aCTD = pattern_aCTD.search(log_content["log"])
                        if not match_aCTD:
                            continue
                        if match_aCTD.group(1) == "carbon.super":
                            match_aI = pattern_aI.search(log_content["log"])
                            match_uI = pattern_uI.search(log_content["log"])
                            tenant = match_aI.group(1)
                            tenantIP = match_uI.group(1)
                            key = tenant + "," + tenantIP
                            timestamp = log_content["time"]
                            tenantDomain[key]["occurence"] += 1
                            if tenantDomain[key]["last_hit"] is None or timestamp > tenantDomain[key]["last_hit"]:
                                tenantDomain[key]["last_hit"] = timestamp
    return dict(tenantDomain)

if __name__ == "__main__":
    recap = get_logs()

    with open('recap_app.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["app", "IP", "occurence", "last_hit"])
        for key, data in recap.items():
            writer.writerow([key.split(",")[0], key.split(",")[1], data["occurence"], data["last_hit"]])

    # print(json.dumps(recap, indent=2))