import json
import re
from collections import defaultdict
from pathlib import Path
import csv

def get_logs():
    tenantDomain = defaultdict(lambda: {"occurence": 0, "last_hit": None, "hit_by_date" : defaultdict(int)})
    # Loki API endpoint
    pattern_aCTD = re.compile(r'apiCreatorTenantDomain=([^,]+)')
    pattern_aI = re.compile(r'applicationId=([^,]+)')
    pattern_uI = re.compile(r'userIp=([^,]+)')

    folder = Path("logs")
    all_dates = set()

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
                            tenantDomain[key]["hit_by_date"][file.name.split(".")[0]] += 1
                        else:
                            continue
    
    for data in tenantDomain.values():
        for date in all_dates:
            data["hit_by_date"].setdefault(date, 0)

    return dict(tenantDomain)

if __name__ == "__main__":
    recap = get_logs()

    all_dates = sorted(next(iter(recap.values()))["hit_by_date"].keys())

    with open('recap_app_daily_empty_included.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["app", "IP", "occurence", "last_hit"] + all_dates)
        for key, data in recap.items():
            row = [key.split(",")[0], key.split(",")[1], data["occurence"], data["last_hit"]]
            row += [data["hit_by_date"][date] for date in all_dates]
            writer.writerow(row)