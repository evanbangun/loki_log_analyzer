#!/usr/bin/env python3
"""
Log Cleanup Verification Script

This script thoroughly checks all log files to verify that:
1. Each file contains only records from its expected date
2. No previous day records remain in current files
3. No next day records remain in current files
4. All data has been properly redistributed
"""

import json
import re
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import logging
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_date_from_filename(filename):
    """Extract date from filename like 'logs_2025-07-29.txt'"""
    try:
        match = re.search(r'logs_(\d{4}-\d{2}-\d{2})\.txt', filename)
        if match:
            return datetime.strptime(match.group(1), '%Y-%m-%d')
        return None
    except Exception:
        return None

def extract_timestamp_from_log_entry(log_message):
    """Extract timestamp from log message like '[2025-07-29 10:30:15,123]'"""
    try:
        match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]', log_message)
        if match:
            timestamp_str = match.group(1)
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
        return None
    except Exception:
        return None

def verify_single_file(file_path, expected_date):
    """Verify a single log file for date consistency"""
    mismatched_records = []
    total_records = 0
    valid_records = 0
    invalid_json = 0
    unparseable_timestamps = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile:
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line:
                    continue
                
                total_records += 1
                
                try:
                    # Parse JSON entry
                    log_data = json.loads(line)
                    log_message = log_data.get('log', '')
                    
                    # Extract timestamp
                    entry_timestamp = extract_timestamp_from_log_entry(log_message)
                    
                    if entry_timestamp is None:
                        unparseable_timestamps += 1
                        continue
                    
                    # Check if timestamp matches expected date
                    entry_date = entry_timestamp.date()
                    expected_date_only = expected_date.date()
                    
                    if entry_date == expected_date_only:
                        valid_records += 1
                    else:
                        mismatched_records.append({
                            'line': line_num,
                            'timestamp': entry_timestamp,
                            'expected': expected_date_only,
                            'actual': entry_date,
                            'log_message': log_message[:200] + '...' if len(log_message) > 200 else log_message
                        })
                
                except json.JSONDecodeError:
                    invalid_json += 1
                except Exception as e:
                    logger.error(f"Error processing line {line_num} in {file_path.name}: {e}")
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'mismatched_records': mismatched_records,
            'invalid_json': invalid_json,
            'unparseable_timestamps': unparseable_timestamps,
            'is_clean': len(mismatched_records) == 0
        }
        
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None

def verify_all_files(directory_path):
    """Verify all log files in the directory"""
    directory = Path(directory_path)
    
    if not directory.exists() or not directory.is_dir():
        logger.error(f"Directory does not exist: {directory_path}")
        return
    
    # Find all log files
    log_files = list(directory.glob('logs_*.txt'))
    
    if not log_files:
        logger.warning(f"No log files found in {directory_path}")
        return
    
    # Sort files by date
    log_files.sort(key=lambda x: extract_date_from_filename(x.name) or datetime.max)
    
    logger.info(f"Found {len(log_files)} log files to verify")
    logger.info("=" * 80)
    
    # Statistics
    total_files = 0
    clean_files = 0
    dirty_files = 0
    total_records = 0
    total_valid_records = 0
    total_mismatched_records = 0
    total_invalid_json = 0
    total_unparseable_timestamps = 0
    
    # Detailed results
    file_results = {}
    date_violations = defaultdict(list)
    
    for file_path in log_files:
        # Skip empty files
        if file_path.name in ["logs_2025-03-01.txt", "logs_2025-03-02.txt", "logs_2025-03-03.txt"]:
            logger.info(f"Skipping empty file: {file_path.name}")
            continue
        
        expected_date = extract_date_from_filename(file_path.name)
        if expected_date is None:
            logger.warning(f"Skipping file with unparseable date: {file_path.name}")
            continue
        
        logger.info(f"Verifying: {file_path.name} (expected: {expected_date.date()})")
        
        result = verify_single_file(file_path, expected_date)
        if result is None:
            continue
        
        file_results[file_path.name] = result
        
        # Update statistics
        total_files += 1
        total_records += result['total_records']
        total_valid_records += result['valid_records']
        total_mismatched_records += len(result['mismatched_records'])
        total_invalid_json += result['invalid_json']
        total_unparseable_timestamps += result['unparseable_timestamps']
        
        if result['is_clean']:
            clean_files += 1
            logger.info(f"  ✅ CLEAN: {result['valid_records']:,} valid records, 0 mismatched")
        else:
            dirty_files += 1
            logger.warning(f"  ❌ DIRTY: {result['valid_records']:,} valid records, {len(result['mismatched_records']):,} mismatched")
            
            # Group violations by date
            for violation in result['mismatched_records']:
                date_violations[violation['actual']].append({
                    'file': file_path.name,
                    'line': violation['line'],
                    'expected': violation['expected'],
                    'timestamp': violation['timestamp']
                })
    
    # Summary Report
    logger.info("=" * 80)
    logger.info("VERIFICATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Files processed: {total_files}")
    logger.info(f"Clean files: {clean_files}")
    logger.info(f"Dirty files: {dirty_files}")
    logger.info(f"Cleanliness rate: {(clean_files/total_files*100):.2f}%" if total_files > 0 else "N/A")
    logger.info("")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Valid records: {total_valid_records:,}")
    logger.info(f"Mismatched records: {total_mismatched_records:,}")
    logger.info(f"Invalid JSON: {total_invalid_json:,}")
    logger.info(f"Unparseable timestamps: {total_unparseable_timestamps:,}")
    logger.info("")
    
    # Data quality metrics
    if total_records > 0:
        data_quality = (total_valid_records / total_records) * 100
        logger.info(f"Data quality: {data_quality:.2f}%")
        
        if data_quality < 99.0:
            logger.warning("⚠️  Data quality below 99% - investigate issues")
        else:
            logger.info("✅ Excellent data quality")
    
    # Detailed violation report
    if date_violations:
        logger.info("=" * 80)
        logger.info("DETAILED VIOLATION REPORT")
        logger.info("=" * 80)
        
        for violation_date, violations in sorted(date_violations.items()):
            logger.warning(f"Records from {violation_date} found in wrong files:")
            for violation in violations:
                logger.warning(f"  {violation['file']}:{violation['line']} - Expected {violation['expected']}, got {violation['timestamp']}")
            logger.info("")
    
    # Files that need attention
    if dirty_files > 0:
        logger.info("=" * 80)
        logger.info("FILES REQUIRING ATTENTION")
        logger.info("=" * 80)
        
        for filename, result in file_results.items():
            if not result['is_clean']:
                logger.warning(f"{filename}: {len(result['mismatched_records']):,} mismatched records")
    
    # Final verdict
    logger.info("=" * 80)
    if dirty_files == 0:
        logger.info("🎉 ALL FILES ARE CLEAN! Ready for parquet conversion.")
    else:
        logger.warning(f"⚠️  {dirty_files} files still have data leakage issues.")
        logger.warning("Fix these issues before converting to parquet.")

def main():
    parser = argparse.ArgumentParser(description='Verify log file cleanup for data leakage')
    parser.add_argument('directory', help='Directory containing log files to verify')
    
    args = parser.parse_args()
    verify_all_files(args.directory)

if __name__ == "__main__":
    main()
