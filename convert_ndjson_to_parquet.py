import requests
import json
import re
from datetime import datetime, timedelta
import time
import pandas as pd
import os
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import gc
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'ndjson_dir': 'logs',
    'parquet_dir': 'SPLP_Logs_parquet_base',
    'batch_size': 10000,  # Process logs in batches
    'compression': 'snappy',
    'backup_dir': 'backup_parquet',
    'num_workers': max(1, (os.cpu_count() or 2) - 1)
}

# Optional faster JSON loader
try:
    import orjson as _orjson
    def fast_json_loads(s: str) -> Any:
        return _orjson.loads(s)
except Exception:  # pragma: no cover - optional dep
    def fast_json_loads(s: str) -> Any:
        return json.loads(s)

# Precompiled regex patterns
TIMESTAMP_RE = re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d+)?)\]')
INT_RE = re.compile(r"[-+]?\d+")
NUMERIC_COLUMNS = ['proxyResponseCode', 'backendLatency', 'requestMediationLatency',
                   'targetResponseCode', 'responseLatency', 'responseMediationLatency']
TIMESTAMP_COLUMNS = ['requestTimestamp', 'time', 'logTime']

def parse_log_content(log_content: Dict) -> Optional[Dict]:
    """Parse the structured log content into a dictionary of fields with validation."""
    try:
        log_message = log_content.get("log", "")
        if not log_message:
            logger.warning("Log message is empty")
            return None
        
        # Extract the full Metric Value block using balanced braces to avoid
        # premature termination when values contain braces (e.g., "/{id}")
        metric_value_str = extract_metric_value_block(log_message)
        if not metric_value_str:
            logger.debug("No metric values found in log")
            return None
        
        metric_values: Dict[str, Any] = {}
        # Split key/value pairs robustly (don't split on commas inside (), {})
        for key, value in split_metric_pairs(metric_value_str):
            metric_values[key] = parse_field_value(key, value)
        
        # Extract logTime from the log content
        log_time = extract_log_time(log_message)
        
        # Build complete record with validation
        record = build_record(log_content, metric_values, log_time)
        
        # Validate required fields
        if not validate_record(record):
            return None
            
        return record
        
    except Exception as e:
        logger.error(f"Error parsing log content: {str(e)}")
        return None

def extract_log_time(log_message: str) -> Optional[datetime]:
    """Extract timestamp from the log message content."""
    try:
        # Look for timestamp pattern at the beginning of log message: [2025-01-07 00:19:26,251]
        match = TIMESTAMP_RE.search(log_message)
        if match:
            timestamp_str = match.group(1)
            # Convert comma to dot for milliseconds (e.g., "00:19:26,251" -> "00:19:26.251")
            timestamp_str = timestamp_str.replace(',', '.')
            return pd.to_datetime(timestamp_str).tz_localize(None)
        
        # If no timestamp found in log content, return None
        logger.debug("No timestamp found in log message")
        return None
        
    except Exception as e:
        logger.warning(f"Failed to extract log time: {e}")
        return None

def extract_metric_value_block(log_message: str) -> Optional[str]:
    """Extract the content inside 'Metric Value: { ... }' using balanced braces.

    This avoids truncation when values themselves contain braces, like '/{id}'.
    Returns the inner text without the outer braces, or None if not found.
    """
    try:
        marker = 'Metric Value:'
        marker_idx = log_message.find(marker)
        if marker_idx == -1:
            return None
        # Find the first opening brace after the marker
        start = log_message.find('{', marker_idx)
        if start == -1:
            return None
        brace_level = 0
        end = None
        for i, ch in enumerate(log_message[start:], start=start):
            if ch == '{':
                brace_level += 1
            elif ch == '}':
                brace_level -= 1
                if brace_level == 0:
                    end = i
                    break
        if end is None or end <= start:
            return None
        return log_message[start + 1:end]
    except Exception as e:
        logger.warning(f"Failed to extract Metric Value block: {e}")
        return None

def split_metric_pairs(metric_value_str: str) -> List[Tuple[str, str]]:
    """Split 'key=value, key2=value2, ...' into pairs without breaking on commas
    inside parentheses or braces.
    """
    pairs: List[Tuple[str, str]] = []
    current: List[str] = []
    paren_level = 0
    brace_level = 0
    for ch in metric_value_str:
        if ch == '(':
            paren_level += 1
        elif ch == ')':
            paren_level = max(paren_level - 1, 0)
        elif ch == '{':
            brace_level += 1
        elif ch == '}':
            brace_level = max(brace_level - 1, 0)
        if ch == ',' and paren_level == 0 and brace_level == 0:
            token = ''.join(current).strip()
            if token:
                key, value = split_first_equals(token)
                if key is not None:
                    pairs.append((key, value))
            current = []
        else:
            current.append(ch)
    # Add the final token
    token = ''.join(current).strip()
    if token:
        key, value = split_first_equals(token)
        if key is not None:
            pairs.append((key, value))
    return pairs

def split_first_equals(token: str) -> Tuple[Optional[str], Optional[str]]:
    """Split a 'key=value' token on the first '=' only."""
    if '=' not in token:
        return None, None
    key, value = token.split('=', 1)
    return key.strip(), value.strip()

def parse_field_value(key: str, value: str) -> Any:
    """Parse individual field values with proper type conversion."""
    try:
        # Normalize string 'null' and empty values to None
        if value is None:
            return None
        lowered = value.strip().lower()
        if lowered in {"null", "none", "nan", ""}:
            return None
        if key in ['proxyResponseCode', 'backendLatency', 'requestMediationLatency', 
                   'targetResponseCode', 'responseLatency', 'responseMediationLatency']:
            value = value.strip()
            return int(value) if INT_RE.fullmatch(value) else None
        elif key in ['responseCacheHit']:
            return value.lower() == 'true'
        elif key in ['requestTimestamp']:
            return pd.to_datetime(value).tz_localize(None) if value else None
        else:
            return str(value) if value else None
    except Exception as e:
        logger.warning(f"Failed to parse field {key}={value}: {e}")
        return None

def build_record(log_content: Dict, metric_values: Dict, log_time: Optional[datetime]) -> Dict:
    """Build complete record with all fields including Log Time."""
    default_values = {
        'apiName': None, 'proxyResponseCode': None, 'errorType': None,
        'destination': None, 'apiCreatorTenantDomain': None, 'platform': None,
        'apiMethod': None, 'apiVersion': None, 'gatewayType': None,
        'apiCreator': None, 'responseCacheHit': None, 'backendLatency': None,
        'correlationId': None, 'requestMediationLatency': None, 'keyType': None,
        'apiId': None, 'applicationName': None, 'targetResponseCode': None,
        'requestTimestamp': None, 'applicationOwner': None, 'userAgent': None,
        'eventType': None, 'apiResourceTemplate': None, 'regionId': None,
        'responseLatency': None, 'responseMediationLatency': None, 'userIp': None,
        'apiContext': None, 'applicationId': None, 'apiType': None,
        'stream': str(log_content.get("stream", "")),
        'time': pd.to_datetime(log_content.get("time")).tz_localize(None) if log_content.get("time") else None,
        'logTime': log_time  # Log Time extracted from log content
    }
    
    default_values.update(metric_values)
    return default_values

def validate_record(record: Dict) -> bool:
    """Validate that record has essential fields."""
    required_fields = ['time', 'apiName']  # Add more as needed
    return all(record.get(field) is not None for field in required_fields)

def write_logs_to_parquet(logs: List[Dict], current_date: datetime, log_dir: str) -> bool:
    """Write logs to parquet with improved error handling and validation."""
    try:
        if not logs:
            logger.warning("No valid logs to write")
            return False
            
        df = pd.DataFrame(logs)
        
        # Clean and validate data
        df = clean_dataframe(df)
        
        if df.empty:
            logger.warning("No valid data after cleaning")
            return False
        
        # Sort by time for consistent ordering
        df = df.sort_values("time", kind="stable")
        
        # Define schema with nullable fields
        schema = define_schema()
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Create partition directory
        partition_path = Path(log_dir) / f'day={current_date.strftime("%Y-%m-%d")}'
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Write parquet file
        parquet_path = partition_path / 'logs.parquet'
        pq.write_table(table, str(parquet_path), compression=CONFIG['compression'])
        
        logger.info(f"Written {len(df)} records to {parquet_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to write parquet file: {e}")
        return False

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate dataframe data."""
    # Handle timestamp columns
    for col in TIMESTAMP_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.floor('us')
    
    # Remove rows with invalid timestamps
    df = df.dropna(subset=['time'])
    
    # Convert numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    return df

def define_schema() -> pa.Schema:
    """Define PyArrow schema with proper nullable types including Log Time."""
    return pa.schema([
        ('apiName', pa.string()),
        ('proxyResponseCode', pa.int64()),
        ('destination', pa.string()),
        ('apiCreatorTenantDomain', pa.string()),
        ('platform', pa.string()),
        ('apiMethod', pa.string()),
        ('apiVersion', pa.string()),
        ('gatewayType', pa.string()),
        ('apiCreator', pa.string()),
        ('responseCacheHit', pa.bool_()),
        ('backendLatency', pa.int64()),
        ('correlationId', pa.string()),
        ('requestMediationLatency', pa.int64()),
        ('keyType', pa.string()),
        ('apiId', pa.string()),
        ('applicationName', pa.string()),
        ('targetResponseCode', pa.int64()),
        ('requestTimestamp', pa.timestamp('us')),
        ('applicationOwner', pa.string()),
        ('userAgent', pa.string()),
        ('eventType', pa.string()),
        ('apiResourceTemplate', pa.string()),
        ('regionId', pa.string()),
        ('responseLatency', pa.int64()),
        ('responseMediationLatency', pa.int64()),
        ('userIp', pa.string()),
        ('apiContext', pa.string()),
        ('applicationId', pa.string()),
        ('apiType', pa.string()),
        ('stream', pa.string()),
        ('time', pa.timestamp('us')),
        ('logTime', pa.timestamp('us'))  # Added Log Time column to schema
    ])

def iter_file_batches(file_path: str, batch_size: int = None):
    """Yield batches of parsed outer JSON logs to manage memory usage."""
    batch_size = batch_size or CONFIG['batch_size']
    current_batch: List[Dict[str, Any]] = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if not line:
                    continue
                try:
                    log_content = fast_json_loads(line.strip())
                    current_batch.append(log_content)
                    if len(current_batch) >= batch_size:
                        yield current_batch
                        current_batch = []
                except Exception as e:
                    logger.warning(f"JSON decode error at line {line_num}: {e}")
                    continue
        if current_batch:
            yield current_batch
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")



def get_date_range():
    """Get date range from user input."""
    try:
        choice = input("1. All Dates \n2. Single Date \n3. Date Range \nTime Range : ")
        if choice == "1":
            return None, None
        elif choice == "2":
            while True:
                try:
                    date_str = input("Enter date (YYYY-MM-DD): ")
                    date = pd.to_datetime(date_str)
                    return date, date
                except ValueError:
                    print("Invalid date format. Please use YYYY-MM-DD format.")
                    sys.exit()
        elif choice == "3":
            while True:
                try:
                    date_range = input("Enter date range (YYYY-MM-DD//YYYY-MM-DD): ")
                    start_date, end_date = date_range.split("//")
                    if start_date > end_date:
                        print("Start date must be before end date. Please try again.")
                        continue
                    return pd.to_datetime(start_date), pd.to_datetime(end_date)
                except ValueError:
                    print("Invalid date format. Please use YYYY-MM-DD format.")
                    sys.exit()
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")
            sys.exit()
    except Exception as e:
        print(f"Error: {str(e)}")
        return None, None

def get_files_to_process(ndjson_dir: str, start_date: Optional[datetime], end_date: Optional[datetime]) -> List[str]:
    """Get list of files to process based on date range."""
    ndjson_files = [f for f in os.listdir(ndjson_dir) if f.endswith('.txt')]
    files_to_process = []
    
    for file_name in ndjson_files:
        try:
            date_str = file_name.replace('logs_', '').replace('.txt', '')
            file_date = pd.to_datetime(date_str)
            
            if start_date is None or (start_date <= file_date <= end_date):
                files_to_process.append(file_name)
        except ValueError:
            logger.warning(f"Skipping file with invalid date format: {file_name}")
            continue
    
    return sorted(files_to_process)

def convert_ndjson_to_parquet() -> int:
    """Main conversion function with improved error handling and progress tracking."""
    ndjson_dir = CONFIG['ndjson_dir']
    parquet_dir = CONFIG['parquet_dir']
    
    # Validate input directory
    if not os.path.exists(ndjson_dir):
        logger.error(f"Input directory {ndjson_dir} does not exist")
        return 0
    
    # Get date range from user
    start_date, end_date = get_date_range()
    
    # Remove backup creation for performance
    
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Get files to process
    files_to_process = get_files_to_process(ndjson_dir, start_date, end_date)
    
    if not files_to_process:
        if start_date is None:
            logger.warning("No files found in the directory")
        elif start_date == end_date:
            logger.warning(f"No files found for date {start_date.strftime('%Y-%m-%d')}")
        else:
            logger.warning(f"No files found in the date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return 0
    
    if start_date is None:
        logger.info(f"Processing all {len(files_to_process)} files")
    elif start_date == end_date:
        logger.info(f"Processing files for date {start_date.strftime('%Y-%m-%d')}")
    else:
        logger.info(f"Processing files from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    total_records = 0
    successful_files = 0
    
    # Initialize a process pool for parsing across all files
    num_workers = CONFIG.get('num_workers') or 1
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for i, file_name in enumerate(files_to_process, 1):
            try:
                logger.info(f"Processing file {i}/{len(files_to_process)}: {file_name}")
                
                date_str = file_name.replace('logs_', '').replace('.txt', '')
                current_date = pd.to_datetime(date_str)
                
                file_path = os.path.join(ndjson_dir, file_name)
                
                # Prepare partition writer
                partition_path = Path(parquet_dir) / f'day={current_date.strftime("%Y-%m-%d")}'
                partition_path.mkdir(parents=True, exist_ok=True)
                parquet_path = partition_path / 'logs.parquet'
                if parquet_path.exists():
                    try:
                        os.remove(parquet_path)
                    except Exception:
                        pass
                schema = define_schema()
                writer: Optional[pq.ParquetWriter] = None
                file_record_count = 0

                # Stream batches, parse in parallel, write incrementally
                for batch in iter_file_batches(file_path):
                    # Parallel parse
                    parsed_dicts = list(executor.map(parse_log_content, batch))
                    parsed_records = [rec for rec in parsed_dicts if rec]
                    if not parsed_records:
                        continue
                    df = pd.DataFrame(parsed_records)
                    df = clean_dataframe(df)
                    if df.empty:
                        continue
                    # Optional stable sort per batch
                    df = df.sort_values("time", kind="stable")
                    table = pa.Table.from_pandas(df, schema=schema)
                    if writer is None:
                        writer = pq.ParquetWriter(str(parquet_path), schema=schema, compression=CONFIG['compression'])
                    writer.write_table(table)
                    file_record_count += len(df)
                    # Clear memory
                    del batch, parsed_dicts, parsed_records, df, table
                    gc.collect()
            except Exception as e:
                logger.error(f"Error processing {file_name}: {e}")
            finally:
                if writer is not None:
                    try:
                        writer.close()
                    except Exception:
                        pass
            if file_record_count > 0:
                total_records += file_record_count
                successful_files += 1
                logger.info(f"Successfully processed {file_name}: {file_record_count} records")
            else:
                logger.warning(f"No valid records found in {file_name}")
    
    logger.info(f"Conversion complete. {successful_files}/{len(files_to_process)} files processed. Total records: {total_records}")
    return total_records

if __name__ == "__main__":
    total = convert_ndjson_to_parquet()
    print(f"\nConversion complete. Total records converted: {total}")
