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

folder = Path("E:/SPLP_Logs")

if not folder.exists() or not any(folder.iterdir()):
    print("'logs' folder tidak ditemukan")
    sys.exit()


CSV_HEADERS = [
    "log_timestamp", "api_name", "proxy_response_code", "error_type", "destination",
    "api_creator_tenant_domain", "platform", "api_method", "api_version", "gateway_type",
    "api_creator", "response_cache_hit", "backend_latency", "correlation_id",
    "request_mediation_latency", "key_type", "api_id", "application_name",
    "target_response_code", "request_timestamp", "application_owner", "user_agent",
    "event_type", "api_resource_template", "response_latency", "region_id",
    "response_mediation_latency", "user_ip", "application_id", "api_type"
]


def iterate_logs(date):
    total_records = 0
    processed_records = 0
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
                    processed_records += 1
                    yield log_content["time"], log_line, file.stem
                except Exception:
                    continue
    print(f"\nTotal records in log: {total_records}")
    print(f"Records Processed: {processed_records}")


def parse_log_line(log_line):
    try:
        return {
            "apiName": re.search(r'apiName=([^,]+)', log_line).group(1) if re.search(r'apiName=([^,]+)', log_line) else None,
            "proxyResponseCode": re.search(r'proxyResponseCode=([^,]+)', log_line).group(1) if re.search(r'proxyResponseCode=([^,]+)', log_line) else None,
            "errorType": re.search(r'errorType=([^,]+)', log_line).group(1) if re.search(r'errorType=([^,]+)', log_line) else None,
            "destination": re.search(r'destination=([^,]+)', log_line).group(1) if re.search(r'destination=([^,]+)', log_line) else None,
            "apiCreatorTenantDomain": re.search(r'apiCreatorTenantDomain=([^,]+)', log_line).group(1) if re.search(r'apiCreatorTenantDomain=([^,]+)', log_line) else None,
            "platform": re.search(r'platform=([^,]+)', log_line).group(1) if re.search(r'platform=([^,]+)', log_line) else None,
            "apiMethod": re.search(r'apiMethod=([^,]+)', log_line).group(1) if re.search(r'apiMethod=([^,]+)', log_line) else None,
            "apiVersion": re.search(r'apiVersion=([^,]+)', log_line).group(1) if re.search(r'apiVersion=([^,]+)', log_line) else None,
            "gatewayType": re.search(r'gatewayType=([^,]+)', log_line).group(1) if re.search(r'gatewayType=([^,]+)', log_line) else None,
            "apiCreator": re.search(r'apiCreator=([^,]+)', log_line).group(1) if re.search(r'apiCreator=([^,]+)', log_line) else None,
            "responseCacheHit": re.search(r'responseCacheHit=([^,]+)', log_line).group(1) if re.search(r'responseCacheHit=([^,]+)', log_line) else None,
            "backendLatency": re.search(r'backendLatency=(\d+)', log_line).group(1) if re.search(r'backendLatency=(\d+)', log_line) else None,
            "correlationId": re.search(r'correlationId=([a-f0-9-]{36})', log_line).group(1) if re.search(r'correlationId=([a-f0-9-]{36})', log_line) else None,
            "requestMediationLatency": re.search(r'requestMediationLatency=(\d+)', log_line).group(1) if re.search(r'requestMediationLatency=(\d+)', log_line) else None,
            "keyType": re.search(r'keyType=([^,]+)', log_line).group(1) if re.search(r'keyType=([^,]+)', log_line) else None,
            "apiId": re.search(r'apiId=([^,]+)', log_line).group(1) if re.search(r'apiId=([^,]+)', log_line) else None,
            "applicationName": re.search(r'applicationName=([^,]+)', log_line).group(1) if re.search(r'applicationName=([^,]+)', log_line) else None,
            "targetResponseCode": re.search(r'targetResponseCode=([^,]+)', log_line).group(1) if re.search(r'targetResponseCode=([^,]+)', log_line) else None,
            "requestTimestamp": re.search(r'requestTimestamp=([^,]+)', log_line).group(1) if re.search(r'requestTimestamp=([^,]+)', log_line) else None,
            "applicationOwner": re.search(r'applicationOwner=([^,]+)', log_line).group(1) if re.search(r'applicationOwner=([^,]+)', log_line) else None,
            "userAgent": re.search(r'userAgent=([^,]+)', log_line).group(1) if re.search(r'userAgent=([^,]+)', log_line) else None,
            "eventType": re.search(r'eventType=([^,]+)', log_line).group(1) if re.search(r'eventType=([^,]+)', log_line) else None,
            "apiResourceTemplate": re.search(r'apiResourceTemplate=([^,]+)', log_line).group(1) if re.search(r'apiResourceTemplate=([^,]+)', log_line) else None,
            "responseLatency": re.search(r'responseLatency=(\d+)', log_line).group(1) if re.search(r'responseLatency=(\d+)', log_line) else None,
            "regionId": re.search(r'regionId=([^,]+)', log_line).group(1) if re.search(r'regionId=([^,]+)', log_line) else None,
            "responseMediationLatency": re.search(r'responseMediationLatency=(\d+)', log_line).group(1) if re.search(r'responseMediationLatency=(\d+)', log_line) else None,
            "userIp": re.search(r'userIp=([^,]+)', log_line).group(1) if re.search(r'userIp=([^,]+)', log_line) else None,
            "applicationId": re.search(r'applicationId=([^,]+)', log_line).group(1) if re.search(r'applicationId=([^,]+)', log_line) else None,
            "apiType": re.search(r'apiType=([^,}]+)', log_line).group(1) if re.search(r'apiType=([^,}]+)', log_line) else None
        }
    except AttributeError as e:
        logging.warning(f"Failed to parse log line: Missing required field - {str(e)}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Unexpected error parsing log line: {str(e)}", exc_info=True)
        return None


def create_log_entry(timestamp, match):
    return {
        "log_timestamp": datetime.fromisoformat(timestamp),
        "api_name": match.get("apiName"),
        "proxy_response_code": match.get("proxyResponseCode"),
        "error_type": match.get("errorType"),
        "destination": match.get("destination"),
        "api_creator_tenant_domain": match.get("apiCreatorTenantDomain"),
        "platform": match.get("platform"),
        "api_method": match.get("apiMethod"),
        "api_version": match.get("apiVersion"),
        "gateway_type": match.get("gatewayType"),
        "api_creator": match.get("apiCreator"),
        "response_cache_hit": match.get("responseCacheHit"),
        "backend_latency": int(match.get("backendLatency", 0)),
        "correlation_id": match.get("correlationId"),
        "request_mediation_latency": int(match.get("requestMediationLatency", 0)),
        "key_type": match.get("keyType"),
        "api_id": match.get("apiId"),
        "application_name": match.get("applicationName"),
        "target_response_code": match.get("targetResponseCode"),
        "request_timestamp": datetime.fromisoformat(match.get("requestTimestamp")),
        "application_owner": match.get("applicationOwner"),
        "user_agent": match.get("userAgent"),
        "event_type": match.get("eventType"),
        "api_resource_template": match.get("apiResourceTemplate"),
        "response_latency": int(match.get("responseLatency", 0)),
        "region_id": match.get("regionId"),
        "response_mediation_latency": int(match.get("responseMediationLatency", 0)),
        "user_ip": match.get("userIp"),
        "application_id": match.get("applicationId"),
        "api_type": match.get("apiType")
    }


def process_daily_logs(date):
    day_writers = {}
    for timestamp, log_line, log_date in iterate_logs(date):
        match = parse_log_line(log_line)
        if match is None:
            continue
        log_entry = create_log_entry(timestamp, match)
        if log_date not in day_writers:
            output_file = f'Processed Logs/{log_date}.csv'
            file = open(output_file, 'w', encoding='utf-8', newline='', buffering=1024*1024)
            writer = csv.writer(file)
            writer.writerow(CSV_HEADERS)
            day_writers[log_date] = (file, writer)
            print(f"Created daily log file: {output_file}")
        day_writers[log_date][1].writerow([log_entry.get(key, "") for key in CSV_HEADERS])
    for file, _ in day_writers.values():
        file.close()


def process_single_file(date):
    file_name = f"logs{('_' + date) if isinstance(date, str) else ('_' + '-'.join(date)) if isinstance(date, tuple) else ''}"
    output_file = f'Processed Logs/{file_name}.csv'
    with open(output_file, 'w', encoding='utf-8', newline='', buffering=1024*1024) as outfile:
        writer = csv.writer(outfile)
        writer.writerow(CSV_HEADERS)
        for timestamp, log_line, log_date in iterate_logs(date):
            match = parse_log_line(log_line)
            if match is None:
                continue
            log_entry = create_log_entry(timestamp, match)
            writer.writerow([log_entry.get(key, "") for key in CSV_HEADERS])
    print(f"Created single log file: {output_file}")


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
                logging.error("Invalid date format. Please use YYYY-MM-DD format.", exc_info=True)
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
                logging.error("Invalid date format. Please use YYYY-MM-DD format.", exc_info=True)
                sys.exit(1)
        else:
            logging.error("Invalid Time Range")
            sys.exit(1)
        partition = input("1. Daily\n2. Single File\nPartition Type : ")
        if partition not in ["1", "2"]:
            logging.error("Invalid Partition Type")
            sys.exit(1)
        Path("Processed Logs").mkdir(exist_ok=True)
        if partition == "1":
            process_daily_logs(date)
        else:
            process_single_file(date)
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)