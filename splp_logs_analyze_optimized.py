import json
import re
from collections import defaultdict
from pathlib import Path
import csv
import sys
import pandas as pd
import openpyxl
from typing import Optional, Tuple, Dict, Any, Union, Generator, List, Set
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, lru_cache
import io
import psutil
import os
import gc
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG level
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants for optimization
CHUNK_SIZE = 1000  # Base chunk size
MEMORY_THRESHOLD = 0.8  # 80% memory usage threshold
MAX_WORKERS = min(32, (os.cpu_count() or 1) + 4)  # Optimal number of workers

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

def get_optimal_chunk_size() -> int:
    """Calculate optimal chunk size based on available memory."""
    available_memory = psutil.virtual_memory().available
    return min(CHUNK_SIZE, max(100, available_memory // (1024 * 1024 * 10)))  # 10MB per chunk

@lru_cache(maxsize=128)
def load_mapping_data() -> Dict[str, str]:
    """Load and validate mapping data from Excel file with caching."""
    try:
        df_mapping = pd.read_excel("map_splp_nasional_institusi.xls")
        return dict(zip(df_mapping["domain"], df_mapping["institution_name"]))
    except Exception as e:
        logging.error(f"Failed to load mapping data: {e}")
        return {}

def cleanup_memory():
    """Force garbage collection and memory cleanup."""
    gc.collect()
    if psutil.virtual_memory().percent > MEMORY_THRESHOLD * 100:
        logging.warning("High memory usage detected, performing cleanup...")
        gc.collect(generation=2)

def process_log_chunk(chunk: List[str], date: Optional[Union[str, Tuple[str, str]]] = None) -> Generator[Dict[str, Any], None, None]:
    """Process a chunk of log lines with optimized pattern matching."""
    for log_record in chunk:
        if not log_record.strip():
            continue
        try:
            log_content = json.loads(log_record)
            log_line = log_content.get("log", "")
            
            # Extract date from the time field in the JSON
            try:
                timestamp = log_content.get("time", "")
                if timestamp:
                    log_date = timestamp.split("T")[0]  # Get YYYY-MM-DD part
                    
                    # Apply date filtering
                    if date is not None:
                        if isinstance(date, tuple):
                            if not (date[0] <= log_date <= date[1]):
                                continue
                        else:  # single date string
                            if log_date != date:
                                continue
                else:
                    continue
            except Exception:
                continue
            
            # Check for carbon.super using the original pattern
            if pattern_apiCTD.search(log_line).group(1) != "carbon.super":
                continue
            
            # Extract all fields using the original patterns
            try:
                fields = {
                    "apiName": pattern_apiN.search(log_line).group(1),
                    "apiCreator": pattern_apiC.search(log_line).group(1),
                    "apiId": pattern_apiI.search(log_line).group(1),
                    "backendLatency": pattern_bL.search(log_line).group(1),
                    "requestMediationLatency": pattern_reqML.search(log_line).group(1),
                    "responseMediationLatency": pattern_resML.search(log_line).group(1),
                    "applicationId": pattern_appI.search(log_line).group(1),
                    "applicationName": pattern_appN.search(log_line).group(1),
                    "applicationOwner": pattern_appO.search(log_line).group(1),
                    "userIp": pattern_uI.search(log_line).group(1),
                    "proxyResponseCode": pattern_pRC.search(log_line).group(1),
                    "targetResponseCode": pattern_tRC.search(log_line).group(1)
                }
            except Exception:
                continue
            
            yield {
                "time": log_content["time"],
                "log_line": log_line,
                "log_date": log_date,
                **fields
            }
                
        except json.JSONDecodeError:
            continue
        except Exception:
            continue

def process_log_file(file_path: Path, date: Optional[Union[str, Tuple[str, str]]] = None) -> Generator[Dict[str, Any], None, None]:
    """Process a single log file with optimized chunked reading."""
    try:
        logging.info(f"Processing file: {file_path.name}")
        with file_path.open("r", encoding="utf-8") as f:
            chunk_size = get_optimal_chunk_size()
            chunk = []
            
            for line in f:
                chunk.append(line)
                if len(chunk) >= chunk_size:
                    yield from process_log_chunk(chunk, date)
                    chunk = []
                    cleanup_memory()
            
            # Process remaining lines
            if chunk:
                yield from process_log_chunk(chunk, date)
                
    except Exception as e:
        logging.error(f"Error reading file {file_path.name}: {e}")

def iterate_logs(date: Optional[Union[str, Tuple[str, str]]] = None) -> Generator[Dict[str, Any], None, None]:
    """Process logs using parallel processing with optimized memory usage."""
    folder = Path("logs")
    if not folder.exists() or not any(folder.iterdir()):
        logging.error("'logs' folder not found or empty")
        sys.exit(1)

    # Filter files by date before processing
    files_to_process = []
    for file_path in folder.iterdir():
        if not file_path.is_file():
            continue
            
        # Extract date from filename (assuming format: logs_YYYY-MM-DD.txt)
        try:
            file_date = file_path.stem.split('_')[1]  # Get the date part after 'logs_'
            if date is not None:
                if isinstance(date, tuple):
                    if not (date[0] <= file_date <= date[1]):
                        continue
                else:  # single date string
                    if file_date != date:
                        continue
            files_to_process.append(file_path)
        except IndexError:
            logging.warning(f"Skipping file with invalid name format: {file_path.name}")
            continue

    total_files = len(files_to_process)
    if total_files == 0:
        logging.warning(f"No files found matching the date criteria: {date}")
        return

    processed_files = 0
    logging.info(f"Found {total_files} files to process")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_log_file, file_path, date) 
                  for file_path in files_to_process]
        
        for future in as_completed(futures):
            try:
                yield from future.result()
                processed_files += 1
                logging.info(f"Progress: {processed_files}/{total_files} files processed")
            except Exception as e:
                logging.error(f"Error in parallel processing: {e}")

@lru_cache(maxsize=32)
def normalize_dates(resultDict: Dict, all_possible_dates: Set[str]) -> List[str]:
    """Normalize dates with caching for better performance."""
    for data in resultDict.values():
        for date in all_possible_dates:
            data["hit_by_date"].setdefault(date, 0)
    return sorted(all_possible_dates)

def optimize_excel_writer(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str, visible_columns: List[str]) -> None:
    """Optimize Excel writer settings for better performance."""
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    worksheet = writer.sheets[sheet_name]
    
    # Optimize column widths - only for visible columns
    for idx, col in enumerate(df.columns):
        if col in visible_columns:
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            )
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 50)

def process_data_chunk(chunk: List[Dict[str, Any]], apiCreator_to_instName: Dict[str, str]) -> List[Dict[str, Any]]:
    """Process a chunk of data with optimized string operations."""
    processed_data = []
    for record in chunk:
        processed_data.append({
            "apiName": record["apiName"],
            "apiCreator": record["apiCreator"],
            "backendLatency": record["backendLatency"],
            "requestMediationLatency": record["requestMediationLatency"],
            "apiId": record["apiId"],
            "applicationName": record["applicationName"],
            "applicationOwner": record["applicationOwner"],
            "responseMediationLatency": record["responseMediationLatency"],
            "applicationId": record["applicationId"]
        })
    return processed_data

def get_logs_allDataset(date: Optional[Union[str, Tuple[str, str]]] = None) -> None:
    """Process all logs and save to CSV using optimized chunked processing."""
    try:
        chunk_size = get_optimal_chunk_size()
        chunk = []
        apiCreator_to_instName = load_mapping_data()
        
        # Create DataFrame with initial data
        df = pd.DataFrame()
        total_records = 0
        processed_records = 0
        
        logging.info(f"Starting to process logs for date: {date}")
        
        # First count total records
        for file_path in Path("logs").iterdir():
            if not file_path.is_file():
                continue
            try:
                file_date = file_path.stem.split('_')[1]
                if date is not None:
                    if isinstance(date, tuple):
                        if not (date[0] <= file_date <= date[1]):
                            continue
                    else:
                        if file_date != date:
                            continue
                with file_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            total_records += 1
            except Exception:
                continue
        
        # Then process records
        for record in iterate_logs(date):
            chunk.append(record)
            processed_records += 1
            
            if len(chunk) >= chunk_size:
                processed_chunk = process_data_chunk(chunk, apiCreator_to_instName)
                if processed_chunk:  # Only count if we got data back
                    temp_df = pd.DataFrame(processed_chunk)
                    df = pd.concat([df, temp_df], ignore_index=True)
                chunk = []
                cleanup_memory()
        
        # Process remaining records
        if chunk:
            processed_chunk = process_data_chunk(chunk, apiCreator_to_instName)
            if processed_chunk:  # Only count if we got data back
                temp_df = pd.DataFrame(processed_chunk)
                df = pd.concat([df, temp_df], ignore_index=True)
        
        print(f"\nTotal records in log: {total_records}")
        print(f"Records with carbon.super: {processed_records}")
        
        # Save to CSV if we have data
        if not df.empty:
            df.to_csv('all_dataset.csv', index=False)
            logging.info(f"Successfully processed {total_records} records, {processed_records} matched the criteria")
        else:
            logging.warning(f"No data found for the selected date range. Processed {total_records} records but none matched the criteria.")
            # Create empty DataFrame with columns to ensure file is created
            empty_df = pd.DataFrame(columns=['apiName', 'apiCreator', 'backendLatency', 
                                           'requestMediationLatency', 'apiId', 
                                           'applicationName', 'applicationOwner', 
                                           'responseMediationLatency', 'applicationId'])
            empty_df.to_csv('all_dataset.csv', index=False)
        
        logging.info("Successfully saved all dataset to CSV")
    except Exception as e:
        logging.error(f"Error in get_logs_allDataset: {e}")
        raise

def get_logs_frequency_by_requester(date: Optional[Union[str, Tuple[str, str]]] = None) -> None:
    """Process logs with optimized memory usage and Excel operations."""
    try:
        resultDict = defaultdict(lambda: {"occurrence": 0, "hit_by_date": defaultdict(int)})
        all_possible_dates = set()
        
        # Process logs in chunks
        for record in iterate_logs(date):
            all_possible_dates.add(record["log_date"])
            if (record["applicationOwner"] != record["apiCreator"] and 
                record["proxyResponseCode"] == "200" and 
                record["targetResponseCode"] == "200"):
                # Optimize string operations
                key_parts = [
                    record["applicationId"],
                    record["applicationName"],
                    record["applicationOwner"],
                    record["apiName"],
                    record["apiCreator"]
                ]
                append_key = ",".join(key_parts)
                resultDict[append_key]["occurrence"] += 1
                resultDict[append_key]["hit_by_date"][record["log_date"]] += 1

        all_dates = normalize_dates(resultDict, all_possible_dates)
        
        # Create DataFrame with optimized memory usage
        data = []
        for key, data_dict in resultDict.items():
            key_parts = key.split(",")
            row = {
                "applicationID": key_parts[0],
                "applicationName": key_parts[1],
                "applicationOwner": key_parts[2],
                "IPPD Requester": apiCreator_to_instName.get(key_parts[2], "Tidak Terdaftar"),
                "apiName": key_parts[3],
                "IPPD pemilik API": apiCreator_to_instName.get(key_parts[4], "Tidak Terdaftar"),
                "Occurrence": data_dict["occurrence"]
            }
            # Add date columns
            for date in all_dates:
                row[date] = data_dict["hit_by_date"][date]
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Save to Excel with optimized settings
        with pd.ExcelWriter("frequency_by_requester.xlsx", engine='openpyxl') as writer:
            visible_columns = ["applicationID", "applicationName", "applicationOwner", 
                             "IPPD Requester", "apiName", "IPPD pemilik API", "Occurrence"]
            optimize_excel_writer(writer, df, "Frequency by Requester", visible_columns)
        
        cleanup_memory()
        logging.info("Successfully saved frequency by requester data")
    except Exception as e:
        logging.error(f"Error in get_logs_frequency_by_requester: {e}")
        raise

def get_logs_integrated_services(date: Optional[Union[str, Tuple[str, str]]] = None) -> None:
    """Process logs with optimized memory usage and Excel operations."""
    try:
        resultDict = defaultdict(lambda: {"occurrence": 0, "hit_by_date": defaultdict(int)})
        all_possible_dates = set()
        
        # Process logs in chunks
        for record in iterate_logs(date):
            all_possible_dates.add(record["log_date"])
            if (record["applicationOwner"] != record["apiCreator"] and 
                record["proxyResponseCode"] == "200" and 
                record["targetResponseCode"] == "200"):
                # Optimize string operations
                key_parts = [record["apiCreator"], record["apiName"]]
                append_key = ",".join(key_parts)
                resultDict[append_key]["occurrence"] += 1
                resultDict[append_key]["hit_by_date"][record["log_date"]] += 1

        all_dates = normalize_dates(resultDict, all_possible_dates)
        
        # Create DataFrame with optimized memory usage
        data = []
        for key, data_dict in resultDict.items():
            key_parts = key.split(",")
            row = {
                "IPPD": apiCreator_to_instName.get(key_parts[0], "Tidak Terdaftar"),
                "apiCreator": key_parts[0],
                "apiName": key_parts[1],
                "Occurrence": data_dict["occurrence"]
            }
            # Add date columns
            for date in all_dates:
                row[date] = data_dict["hit_by_date"][date]
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Save to Excel with optimized settings
        with pd.ExcelWriter("integrated_services_frequency.xlsx", engine='openpyxl') as writer:
            visible_columns = ["IPPD", "apiCreator", "apiName", "Occurrence"]
            optimize_excel_writer(writer, df, "Integrated Services Frequency", visible_columns)
        
        cleanup_memory()
        logging.info("Successfully saved integrated services frequency data")
    except Exception as e:
        logging.error(f"Error in get_logs_integrated_services: {e}")
        raise

if __name__ == "__main__":
    try:
        # Load mapping data at startup
        apiCreator_to_instName = load_mapping_data()
        if not apiCreator_to_instName:
            logging.error("Failed to load mapping data. Exiting...")
            sys.exit(1)

        time_range = input("1. All Date\n2. Single Date\n3. Date Range\nTime Range : ")
        if time_range == "1":
            date = None
        elif time_range == "2":
            date = input("Enter Date (YYYY-MM-DD) : ")
            # Validate single date format
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