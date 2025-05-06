import json
import re
from collections import defaultdict
from pathlib import Path
import csv

def get_logs():
    tenantDomain = defaultdict(lambda: {"occurence": 0, "last_hit": None, "hit_by_date" : defaultdict(int)})
    # Loki API endpoint
    pattern_aCTD = re.compile(r'apiCreatorTenantDomain=([^,]+)')
    pattern_aC = re.compile(r'apiCreator=([^,]+)')

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
                            match_aC = pattern_aC.search(log_content["log"])
                            tenant = match_aC.group(1)
                            timestamp = log_content["time"]
                            tenantDomain[tenant]["occurence"] += 1
                            if tenantDomain[tenant]["last_hit"] is None or timestamp > tenantDomain[tenant]["last_hit"]:
                                tenantDomain[tenant]["last_hit"] = timestamp
                            tenantDomain[tenant]["hit_by_date"][file.name.split(".")[0]] += 1
                        else:
                            continue
    all_dates = set()
    for data in tenantDomain.values():
        all_dates.update(data["hit_by_date"].keys())
    
    for data in tenantDomain.values():
        for date in all_dates:
            data["hit_by_date"].setdefault(date, 0)

    return dict(tenantDomain)

if __name__ == "__main__":
    recap = get_logs()

    # Extract all unique dates (sorted)
    all_dates = sorted(next(iter(recap.values()))["hit_by_date"].keys())

    with open('recap_domain_daily.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["domain", "occurence", "last_hit"] + all_dates)
        for domain, data in recap.items():
            row = [domain, data["occurence"], data["last_hit"]]
            row += [data["hit_by_date"][date] for date in all_dates]
            writer.writerow(row)

    print(json.dumps(recap, indent=2))