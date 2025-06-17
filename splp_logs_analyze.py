import json
import re
from collections import defaultdict
from pathlib import Path
import csv
import sys
import pandas as pd
import openpyxl
import logging
import os
from datetime import datetime

#API Owner pattern
pattern_apiN = re.compile(r'apiName=([^,]+)')
pattern_apiC = re.compile(r'apiCreator=([^,]+)')
pattern_apiCTD = re.compile(r'apiCreatorTenantDomain=([^,]+)')
pattern_apiI = re.compile(r'apiId=([^,]+)')

#Latency Pattern
pattern_bL = re.compile(r'backendLatency=(\d+)')
pattern_reqML = re.compile(r'requestMediationLatency=(\d+)')
pattern_resML = re.compile(r'responseMediationLatency=(\d+)')

#API Requester pattern
pattern_appI = re.compile(r'applicationId=([^,]+)')
pattern_appN = re.compile(r'applicationName=([^,]+)')
pattern_appO = re.compile(r'applicationOwner=([^,]+)')
pattern_uI = re.compile(r'userIp=([^,]+)')

#Response Code
pattern_pRC = re.compile(r'proxyResponseCode=([^,]+)')
pattern_tRC = re.compile(r'targetResponseCode=([^,]+)')

# folder = Path("logs")
folder = Path("E:/SPLP_Logs")

if not folder.exists() or not any(folder.iterdir()):
    print("folder berisi logs tidak ditemukan")
    sys.exit()

df_mapping = pd.read_excel("mapping.xlsx")
mapping_dict = {}

def data_cleansing(log_line):
    clean_word = ["dummy", "admin", "bimtek", "demo", "internal-key-app", "test"]
    return any(word in log_line for word in clean_word)

def fuzzy_lookup(lookup_dict, search_key):
    if not lookup_dict:
        return "Tidak Terdaftar"
    for pattern_key, inst_name in lookup_dict.items():
        if not isinstance(pattern_key, str):
            continue
        if pattern_key in search_key:
            return inst_name
    return "Tidak Terdaftar"

def iterate_logs(date, iL, cleanse_data):
    total_records = 0
    processed_records = 0
    if iL not in ["1", "2"]:
        logging.error("Invalid Interoperability Level")
        sys.exit(1)
    for file in folder.iterdir():
        if not file.is_file():
            continue
        if date is not None:
            if isinstance(date, tuple):
                if not (date[0] <= file.stem.split('_')[1] <= date[1]):
                    continue
            else:  
                if file.stem.split('_')[1] != date:
                    continue
        print("iterating through file : ", file.name)
        with file.open("r", encoding="utf-8") as f:
            for log_record in f:
                if not log_record.strip():
                    continue
                try:
                    total_records += 1  
                    log_content = json.loads(log_record)
                    log_line = log_content["log"]
                    if (iL == "1" and pattern_apiCTD.search(log_line).group(1) != "carbon.super") or (iL == "2" and pattern_apiCTD.search(log_line).group(1) == "carbon.super"):
                        continue
                    if cleanse_data and data_cleansing(str(log_line)):
                        continue
                    processed_records += 1
                    yield log_content["time"], log_line, file.stem
                except Exception:
                    continue
    print(f"\nTotal records in log: {total_records}")
    print(f"Records Processed: {processed_records}")

def parse_log_line(log_line):
    try:
        return {
                "apiName" : pattern_apiN.search(log_line).group(1) if pattern_apiN.search(log_line) else None,
                "apiCreator" : pattern_apiC.search(log_line).group(1) if pattern_apiC.search(log_line) else None,
                "apiId" : pattern_apiI.search(log_line).group(1) if pattern_apiI.search(log_line) else None,
                "apiCreatorTenantDomain" : pattern_apiCTD.search(log_line).group(1) if pattern_apiCTD.search(log_line) else None,
                "backendLatency" : pattern_bL.search(log_line).group(1) if pattern_bL.search(log_line) else None,
                "requestMediationLatency" : pattern_reqML.search(log_line).group(1) if pattern_reqML.search(log_line) else None,
                "responseMediationLatency" : pattern_resML.search(log_line).group(1) if pattern_resML.search(log_line) else None,
                "applicationId" : pattern_appI.search(log_line).group(1) if pattern_appI.search(log_line) else None,
                "applicationName" : pattern_appN.search(log_line).group(1) if pattern_appN.search(log_line) else None,
                "applicationOwner" : pattern_appO.search(log_line).group(1) if pattern_appO.search(log_line) else None,
                "userIp" : pattern_uI.search(log_line).group(1) if pattern_uI.search(log_line) else None,
                "proxyResponseCode" : pattern_pRC.search(log_line).group(1) if pattern_pRC.search(log_line) else None,
                "targetResponseCode" : pattern_tRC.search(log_line).group(1) if pattern_tRC.search(log_line) else None
            }
    except AttributeError as e:
        logging.warning(f"Failed to parse log line: Missing required field - {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error parsing log line: {str(e)}")
        return None

def normalize_dates(resultDict, all_possible_dates):
    for data in resultDict.values():
        for date in all_possible_dates:
            data["hit_by_date"].setdefault(date, 0)
    return sorted(all_possible_dates)

def get_logs_allDataset(date, iL, cleanse_data):
    resultDict = []
    file_name = f"all_dataset{('_' + date) if isinstance(date, str) else ('_' + '-'.join(date)) if isinstance(date, tuple) else ''}_{'National' if iL == '1' else 'Internal'}"
    for timestamp, log_line, log_date in iterate_logs(date, iL, cleanse_data):
        match = parse_log_line(log_line)
        if match is None:
            continue
        resultDict.append({
            "apiName" : match["apiName"], 
            "apiCreator" : match["apiCreator"], 
            "backendLatency" : match["backendLatency"], 
            "requestMediationLatency" : match["requestMediationLatency"], 
            "apiId" : match["apiId"], 
            "applicationName" : match["applicationName"], 
            "applicationOwner" : match["applicationOwner"],
            "responseMediationLatency" : match["responseMediationLatency"],
            "applicationId" : match["applicationId"]
        })
    with open(f'Report/{file_name}.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["apiName", "apiCreator", "backendLatency", "requestMediationLatency", "apiId", "applicationName", "applicationOwner","responseMediationLatency", "applicationId"])
        for data in resultDict:
            writer.writerow([data["apiName"], data["apiCreator"], data["backendLatency"], data["requestMediationLatency"], data["apiId"], data["applicationName"], data["applicationOwner"], data["responseMediationLatency"], data["applicationId"]])


def recap(date, iL, cleanse_data):
    resultDict = defaultdict(lambda: {"occurrence": 0, "hit_by_date": defaultdict(int)})
    all_possible_dates = set()
    view_type = input("Choose view type:\n1. Aggregated \n2. Daily \nView Type: ")
    file_name = f"recap_{('_' + date) if isinstance(date, str) else ('_' + '-'.join(date)) if isinstance(date, tuple) else ''}_{'National' if iL == '1' else 'Internal'}_{'Aggregated' if view_type == '1' else 'Daily'}"
    for timestamp, log_line, log_date in iterate_logs(date, iL, cleanse_data):
        match = parse_log_line(log_line)
        if match is None:
            continue
        if match["applicationOwner"] != match["apiCreator"] and match["proxyResponseCode"] == "200" and match["targetResponseCode"] == "200":
            append_key = (match["apiCreator"], match["apiName"], match["applicationOwner"], match["applicationName"], match["userIp"], match["apiCreatorTenantDomain"])
            resultDict[append_key]["occurrence"] += 1
            resultDict[append_key]["hit_by_date"][log_date] += 1
            all_possible_dates.add(log_date)
    all_dates = normalize_dates(resultDict, all_possible_dates)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Recap"
    if view_type == "1":
        ws.append(["Instansi Pemilik API", "apiCreator", "apiName", "Instansi API Requester", "apiCreatorTenantDomain", "applicationOwner", "applicationName", "userIp", "Occurrence"])
        for key, data in resultDict.items():
            row = [fuzzy_lookup(mapping_dict, key[0]), key[0], key[1], fuzzy_lookup(mapping_dict, key[2]), key[5], key[2], key[3], key[4], data["occurrence"]]
            ws.append(row)
    elif view_type == "2":
        ws.append(["Instansi Pemilik API", "apiCreator", "apiName", "Instansi API Requester", "apiCreatorTenantDomain", "applicationOwner", "applicationName", "userIp", "Occurrence"] + all_dates)
        for key, data in resultDict.items():
            row = [fuzzy_lookup(mapping_dict, key[0]), key[0], key[1], fuzzy_lookup(mapping_dict, key[2]), key[5], key[2], key[3], key[4], data["occurrence"]]
            row += [data["hit_by_date"][date] for date in all_dates]
            ws.append(row)
    else:
        logging.error("Invalid view type selected")
        sys.exit(1)
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
    wb.save(f"Report/{file_name}.xlsx")

def calculate_max_concurrent_hits(date, iL, cleanse_data):
    hits_per_second = defaultdict(int)
    max_hits = 0
    max_hits_timestamp = None
    print("Calculating concurrent hits...")
    for timestamp, log_line, log_date in iterate_logs(date, iL, cleanse_data):
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            second_key = dt.strftime('%Y-%m-%d %H:%M:%S')
            hits_per_second[second_key] += 1
            if hits_per_second[second_key] > max_hits:
                max_hits = hits_per_second[second_key]
                max_hits_timestamp = second_key
        except Exception as e:
            logging.warning(f"Failed to process timestamp: {timestamp} - {str(e)}")
            continue
    file_name = f"concurrent_hits_{('_' + date) if isinstance(date, str) else ('_' + '-'.join(date)) if isinstance(date, tuple) else ''}_{'National' if iL == '1' else 'Internal'}"
    with open(f'Report/{file_name}.csv', 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Timestamp', 'Hits'])
        for timestamp, hits in sorted(hits_per_second.items()):
            writer.writerow([timestamp, hits])
    print(f"\nMaximum concurrent hits: {max_hits}")
    print(f"Timestamp of maximum hits: {max_hits_timestamp}")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Concurrent Hits Summary"
    ws.append(['Metric', 'Value'])
    ws.append(['Maximum Concurrent Hits', max_hits])
    ws.append(['Timestamp of Maximum Hits', max_hits_timestamp])
    ws.append(['Total Unique Seconds', len(hits_per_second)])
    ws.append(['Average Hits per Second', sum(hits_per_second.values()) / len(hits_per_second)])
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
    wb.save(f"Report/{file_name}_summary.xlsx")

    
if __name__ == "__main__":
    report_dir = Path("Report")
    if not report_dir.exists():
        os.makedirs(report_dir)
    if not Path("mapping.xlsx").exists():
        logging.error("mapping.xlsx file not found")
        sys.exit(1)
    time_range = input("1. All Date\n2. Single Date\n3. Date Range\nTime Range : ")
    if time_range == "1":
        date = None
    elif time_range == "2":
        date = input("Enter Date (YYYY-MM-DD) : ")
        try:
            pd.to_datetime(date)
        except ValueError:
            logging.error("Invalid date format. Please use YYYY-MM-DD format.")
            sys.exit(1)
    elif time_range == "3":
        date = input("Enter Date Range (YYYY-MM-DD//YYYY-MM-DD) : ")
        try:
            start_date = pd.to_datetime(date.split("//")[0])
            end_date = pd.to_datetime(date.split("//")[1])
            if start_date >= end_date:
                logging.error("Invalid Date Range: Start date must be before end date")
                sys.exit(1)
            date = (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        except ValueError:
            logging.error("Invalid date format. Please use YYYY-MM-DD format.")
            sys.exit(1)
    else:
        logging.error("Invalid Time Range")
        sys.exit(1)
    iL = input("1. National\n2. Internal\nInteroperability Level : ")
    if iL == "1":
        mapping_dict = dict(zip(df_mapping["Akun Nasional"], df_mapping["Nama Instansi"]))
    elif iL == "2":
        mapping_dict = dict(zip(df_mapping["Domain"], df_mapping["Nama Instansi"]))
    else:
        logging.error("Invalid Log Type")
        sys.exit(1)
    cleanse_data_input = input("Cleanse Data ? (Y/n): ")
    if cleanse_data_input.lower() not in ['y', 'n']:
        logging.error("Invalid Input")
        sys.exit(1)
    else:
        cleanse_data = cleanse_data_input.lower() == "y"
    log_type = input("1. All Dataset\n2. Recap\n3. Concurrent Hits\nLog Type : ")
    if log_type == "1":
        get_logs_allDataset(date, iL, cleanse_data)
    elif log_type == "2":
        recap(date, iL, cleanse_data)
    elif log_type == "3":
        calculate_max_concurrent_hits(date, iL, cleanse_data)
    else:
        logging.error("Invalid Log Type")
        sys.exit(1)