import json
import re
from collections import defaultdict
from pathlib import Path
import csv
import sys
import pandas as pd
import openpyxl

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

def iterate_logs():
    for file in folder.iterdir():
        if not file.is_file():
            continue
        print("iterating through file : ", file.name)
        with file.open("r", encoding="utf-8") as f:
            for log_record in f:
                if not log_record.strip():
                    continue
                try:
                    log_content = json.loads(log_record)
                    log_line = log_content["log"]
                    if pattern_apiCTD.search(log_line).group(1) != "carbon.super":
                        continue
                    yield log_content["time"], log_line, file.stem
                except Exception:
                    continue


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


def get_logs_allDataset():
    resultDict = []
    for timestamp, log_line, log_date in iterate_logs():
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
    # for file in folder.iterdir():
    #     if file.is_file():
    #         print("iterating through file : ", file.name)
    #         with file.open("r", encoding="utf-8") as f:
    #             for log_record in f:
    #                 if log_record.strip():
    #                     try:
    #                         log_content = json.loads(log_record)
    #                         match_apiCTD = pattern_apiCTD.search(log_content["log"]).group(1)
    #                     except Exception as e:
    #                         continue
    #                     if match_apiCTD == "carbon.super":
    #                         timestamp = log_content["time"]
    #                         match = parse_log_line(log_content["log"])
    #                         if match is None:
    #                             continue
    #                         resultDict.append({
    #                                             "apiName" : match["apiName"], 
    #                                             "apiCreator" : match["apiCreator"], 
    #                                             "backendLatency" : match["backendLatency"], 
    #                                             "requestMediationLatency" : match["requestMediationLatency"], 
    #                                             "apiId" : match["apiId"], 
    #                                             "applicationName" : match["applicationName"], 
    #                                             "applicationOwner" : match["applicationOwner"],
    #                                             "responseMediationLatency" : match["responseMediationLatency"],
    #                                             "applicationId" : match["applicationId"]
    #                                         })
    # wb = openpyxl.Workbook()
    # ws = wb.active
    # ws.title = "All Dataset"
    # ws.append(["apiName", "apiCreator", "backendLatency", "requestMediationLatency", "apiId", "applicationName", "applicationOwner","responseMediationLatency", "applicationId"])
    # for data in resultDict:
    #     ws.append([data["apiName"], data["apiCreator"], data["backendLatency"], data["requestMediationLatency"], data["apiId"], data["applicationName"], data["applicationOwner"], data["responseMediationLatency"], data["applicationId"]])
    # wb.save("all_dataset.xlsx")
    with open('all_dataset.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["apiName", "apiCreator", "backendLatency", "requestMediationLatency", "apiId", "applicationName", "applicationOwner","responseMediationLatency", "applicationId"])
        for data in resultDict:
            writer.writerow([data["apiName"], data["apiCreator"], data["backendLatency"], data["requestMediationLatency"], data["apiId"], data["applicationName"], data["applicationOwner"], data["responseMediationLatency"], data["applicationId"]])


def get_logs_frequency_by_requester():
    resultDict = defaultdict(lambda: {"occurrence": 0, "hit_by_date" : defaultdict(int)})
    all_possible_dates = set()
    for timestamp, log_line, log_date in iterate_logs():
        match = parse_log_line(log_line)
        if match is None:
            continue
        if match["applicationOwner"] != match["apiCreator"] and match["proxyResponseCode"] == "200" and match["targetResponseCode"] == "200":
            append_key = ",".join([match["applicationId"], match["applicationName"], match["applicationOwner"],  match["apiName"], match["apiCreator"]])
            resultDict[append_key]["occurrence"] += 1
            resultDict[append_key]["hit_by_date"][log_date] += 1
    # for file in folder.iterdir():
    #     if file.is_file():
    #         all_possible_dates.add(file.name.split(".")[0])
    #         print("iterating through file : ", file.name)
    #         with file.open("r", encoding="utf-8") as f:
    #             for log_record in f:
    #                 if log_record.strip():
    #                     try:
    #                         log_content = json.loads(log_record)
    #                         match_apiCTD = pattern_apiCTD.search(log_content["log"]).group(1)
    #                     except Exception as e:
    #                         continue
    #                     if match_apiCTD == "carbon.super":
    #                         timestamp = log_content["time"]
    #                         match = parse_log_line(log_content["log"])
    #                         if match is None:
    #                             continue
    #                         if match["applicationOwner"] != match["apiCreator"] and match["proxyResponseCode"] == "200" and match["targetResponseCode"] == "200":
    #                             append_key = ",".join([match["applicationId"], match["applicationName"], match["applicationOwner"],  match["apiName"], match["apiCreator"]])
    #                             resultDict[append_key]["occurrence"] += 1
    #                             resultDict[append_key]["hit_by_date"][file.name.split(".")[0]] += 1
    all_dates = normalize_dates(resultDict, all_possible_dates)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Frequency by Requester"
    ws.append(["applicationID", "applicationName", "applicationOwner", "IPPD Requester", "apiName", "IPPD pemilik API", "Occurrence"] + all_dates)
    for key, data in resultDict.items():
        key_parts = key.split(",")
        row = [key_parts[0], key_parts[1], key_parts[2], apiCreator_to_instName.get(key_parts[2], "Tidak Terdaftar"), key_parts[3], apiCreator_to_instName.get(key_parts[4], "Tidak Terdaftar"), data["occurrence"]]
        row += [data["hit_by_date"][date] for date in all_dates]
        ws.append(row)
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
    wb.save("frequency_by_requester.xlsx")
    # with open('frequency_by_requester.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
    #     writer = csv.writer(outfile)
    #     writer.writerow(["applicationID", "applicationName", "applicationOwner", "apiName", "Occurrence"] + all_dates)
    #     for key, data in resultDict.items():
    #         key_parts = key.split(",")
    #         row = [key_parts[0], key_parts[1], key_parts[2], key_parts[3], data["occurrence"]]
    #         row += [data["hit_by_date"][date] for date in all_dates]
    #         writer.writerow(row)


def get_logs_integrated_services():
    resultDict = defaultdict(lambda: {"occurrence": 0, "hit_by_date" : defaultdict(int)})
    all_possible_dates = set()
    for timestamp, log_line, log_date in iterate_logs():
        match = parse_log_line(log_line)
        if match is None:
            continue
        if match["applicationOwner"] != match["apiCreator"] and match["proxyResponseCode"] == "200" and match["targetResponseCode"] == "200":
            append_key = ",".join([match["apiCreator"], match["apiName"]])
            resultDict[append_key]["occurrence"] += 1
            resultDict[append_key]["hit_by_date"][log_date] += 1
    # for file in folder.iterdir():
    #     if file.is_file():
    #         all_possible_dates.add(file.name.split(".")[0])
    #         print("iterating through file : ", file.name)
    #         with file.open("r", encoding="utf-8") as f:
    #             for log_record in f:
    #                 if log_record.strip():
    #                     try:
    #                         log_content = json.loads(log_record)
    #                         match_apiCTD = pattern_apiCTD.search(log_content["log"]).group(1)
    #                     except Exception as e:
    #                         continue
    #                     if match_apiCTD == "carbon.super":
    #                         timestamp = log_content["time"]
    #                         match = parse_log_line(log_content["log"])
    #                         if match is None:
    #                             continue
    #                         if match["applicationOwner"] != match["apiCreator"] and match["proxyResponseCode"] == "200" and match["targetResponseCode"] == "200":
    #                             append_key = ",".join([match["apiCreator"], match["apiName"]])
    #                             resultDict[append_key]["occurrence"] += 1
    #                             resultDict[append_key]["hit_by_date"][file.name.split(".")[0]] += 1
    all_dates = normalize_dates(resultDict, all_possible_dates)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Integrated Services Frequency"
    ws.append(["IPPD", "apiCreator", "apiName", "Occurrence"] + all_dates)
    for key, data in resultDict.items():
        key_parts = key.split(",")
        row = [apiCreator_to_instName.get(key_parts[0], "Tidak Terdaftar"), key_parts[0], key_parts[1], data["occurrence"]]
        row += [data["hit_by_date"][date] for date in all_dates]
        ws.append(row)
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
    wb.save("integrated_services_frequency.xlsx")
    # with open('integrated_services_frequency.csv', 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
    #     writer = csv.writer(outfile)
    #     writer.writerow(["IPPD", "apiCreator", "apiName", "Occurrence"] + all_dates)
    #     for key, data in resultDict.items():
    #         key_parts = key.split(",")
    #         row = [apiCreator_to_instName.get(key_parts[0], "Tidak Terdaftar"), key_parts[0], key_parts[1], data["occurrence"]]
    #         row += [data["hit_by_date"][date] for date in all_dates]
    #         writer.writerow(row)


if __name__ == "__main__":
    log_type = input("1. All Dataset\n2. Seberapa Sering Suatu Instansi melakukan hit API ke SPLP\n3. Jumlah Layanan Terintegrasi\nLog Type : ")
    if log_type == "1":
        get_logs_allDataset()
    elif log_type == "2":
        get_logs_frequency_by_requester()
    elif log_type == "3":
        get_logs_integrated_services()
    