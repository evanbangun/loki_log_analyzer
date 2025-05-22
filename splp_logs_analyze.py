import json
import re
from collections import defaultdict
from pathlib import Path
import csv
import sys
import pandas as pd
import openpyxl
import logging

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

folder = Path("logs")
if not folder.exists() or not any(folder.iterdir()):
    print("'logs' folder tidak ditemukan")
    sys.exit()

df_mapping = pd.read_excel("map_splp_nasional_institusi.xls")
apiCreator_to_instName = dict(zip(df_mapping["domain"], df_mapping["institution_name"]))

def iterate_logs(date):
    total_records = 0
    processed_records = 0
    
    for file in folder.iterdir():
        if not file.is_file():
            continue
        if date is not None:
            if isinstance(date, tuple):  # date range
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
                    total_records += 1  # Count ALL records before any filtering
                    log_content = json.loads(log_record)
                    log_line = log_content["log"]
                    if pattern_apiCTD.search(log_line).group(1) != "carbon.super":
                        continue
                    processed_records += 1  # Count only carbon.super records
                    yield log_content["time"], log_line, file.stem
                except Exception:
                    continue
    
    print(f"\nTotal records in log: {total_records}")
    print(f"Records with carbon.super: {processed_records}")

def parse_log_line(log_line):
    try:
        return {
            "apiName" : pattern_apiN.search(log_line).group(1),
            "apiCreator" : pattern_apiC.search(log_line).group(1),
            "apiId" : pattern_apiI.search(log_line).group(1),
            "backendLatency" : pattern_bL.search(log_line).group(1),
            "requestMediationLatency" : pattern_reqML.search(log_line).group(1),
            "responseMediationLatency" : pattern_resML.search(log_line).group(1),
            "applicationId" : pattern_appI.search(log_line).group(1),
            "applicationName" : pattern_appN.search(log_line).group(1),
            "applicationOwner" : pattern_appO.search(log_line).group(1),
            "userIp" : pattern_uI.search(log_line).group(1),
            "proxyResponseCode" : pattern_pRC.search(log_line).group(1),
            "targetResponseCode" : pattern_tRC.search(log_line).group(1),
        }
    except Exception as e:
        print(e)
        return None

def normalize_dates(resultDict, all_possible_dates):
    for data in resultDict.values():
        for date in all_possible_dates:
            data["hit_by_date"].setdefault(date, 0)
    return sorted(all_possible_dates)


def get_logs_allDataset(date):
    resultDict = []
    
    for timestamp, log_line, log_date in iterate_logs(date):
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
    
    with open('all_dataset.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["apiName", "apiCreator", "backendLatency", "requestMediationLatency", "apiId", "applicationName", "applicationOwner","responseMediationLatency", "applicationId"])
        for data in resultDict:
            writer.writerow([data["apiName"], data["apiCreator"], data["backendLatency"], data["requestMediationLatency"], data["apiId"], data["applicationName"], data["applicationOwner"], data["responseMediationLatency"], data["applicationId"]])


def get_logs_frequency_by_requester(date):
    resultDict = defaultdict(lambda: {"occurrence": 0, "hit_by_date" : defaultdict(int)})
    all_possible_dates = set()
    for timestamp, log_line, log_date in iterate_logs(date):
        match = parse_log_line(log_line)
        if match is None:
            continue
        all_possible_dates.add(log_date)
        if match["applicationOwner"] != match["apiCreator"] and match["proxyResponseCode"] == "200" and match["targetResponseCode"] == "200":
            # append_key = ",".join([match["applicationId"], match["applicationName"], match["applicationOwner"],  match["apiName"], match["apiCreator"]])
            append_key = (match["applicationId"], match["applicationName"], match["applicationOwner"],  match["apiName"], match["apiCreator"])
            resultDict[append_key]["occurrence"] += 1
            resultDict[append_key]["hit_by_date"][log_date] += 1
    all_dates = normalize_dates(resultDict, all_possible_dates)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Frequency by Requester"
    ws.append(["applicationID", "applicationName", "applicationOwner", "IPPD Requester", "apiName", "IPPD pemilik API", "Occurrence"] + all_dates)
    for key, data in resultDict.items():
        row = [key[0], key[1], key[2], apiCreator_to_instName.get(key[2], "Tidak Terdaftar"), key[3], apiCreator_to_instName.get(key[4], "Tidak Terdaftar"), data["occurrence"]]
        row += [data["hit_by_date"][date] for date in all_dates]
        ws.append(row)
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
    wb.save("frequency_by_requester.xlsx")


def get_logs_integrated_services(date):
    resultDict = defaultdict(lambda: {"occurrence": 0, "hit_by_date" : defaultdict(int)})
    all_possible_dates = set()
    for timestamp, log_line, log_date in iterate_logs(date):
        match = parse_log_line(log_line)
        if match is None:
            continue
        all_possible_dates.add(log_date)
        if match["applicationOwner"] != match["apiCreator"] and match["proxyResponseCode"] == "200" and match["targetResponseCode"] == "200":
            # append_key = ",".join([match["apiCreator"], match["apiName"]])
            append_key = (match["apiCreator"], match["apiName"])
            resultDict[append_key]["occurrence"] += 1
            resultDict[append_key]["hit_by_date"][log_date] += 1
    all_dates = normalize_dates(resultDict, all_possible_dates)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Integrated Services Frequency"
    ws.append(["IPPD", "apiCreator", "apiName", "Occurrence"] + all_dates)
    for key, data in resultDict.items():
        row = [apiCreator_to_instName.get(key[0], "Tidak Terdaftar"), key[0], key[1], data["occurrence"]]
        row += [data["hit_by_date"][date] for date in all_dates]
        ws.append(row)
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
    wb.save("integrated_services_frequency.xlsx")


if __name__ == "__main__":
    try:
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
            start_date, end_date = date.split("//")
            try:
                start_date = pd.to_datetime(start_date)
                end_date = pd.to_datetime(end_date)
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

        log_type = input("1. All Dataset\n2. Seberapa Sering Suatu Instansi melakukan hit API ke SPLP\n3. Jumlah Layanan Terintegrasi\nLog Type : ")
        if log_type == "1":
            get_logs_allDataset(date)
        elif log_type == "2":
            get_logs_frequency_by_requester(date)
        elif log_type == "3":
            get_logs_integrated_services(date)
        else:
            logging.error("Invalid Log Type")
            sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
    