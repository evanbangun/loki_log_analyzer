#!/usr/bin/env python3
"""
Safe Log Data Redistributor - Append-Only Approach

This script handles data redistribution safely by:
1. Appending entries to target files (no full rewrites)
2. Removing only specific entries from source files
3. Using temporary files for atomic operations
4. Handling large files efficiently
"""

import json
import re
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import logging
import shutil
import tempfile

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

def append_entries_to_file(file_path, entries, dry_run=False):
    """Append entries to a file (creates file if it doesn't exist)"""
    if not entries:
        return
    
    if dry_run:
        logger.info(f"  Would append {len(entries):,} entries to {file_path.name}")
        return
    
    try:
        # Create directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Append entries to file
        with open(file_path, 'a', encoding='utf-8') as outfile:
            for entry in entries:
                outfile.write(entry + '\n')
        
        logger.info(f"  Appended {len(entries):,} entries to {file_path.name}")
    except Exception as e:
        logger.error(f"Error appending to {file_path}: {e}")
        raise

def remove_entries_from_file(file_path, entries_to_remove, dry_run=False):
    """Remove specific entries from a file using temporary file for safety"""
    if not entries_to_remove:
        return 0
    
    if dry_run:
        logger.info(f"  Would remove {len(entries_to_remove):,} entries from {file_path.name}")
        return len(entries_to_remove)
    
    try:
        # Create a set for fast lookup
        entries_to_remove_set = set(entries_to_remove)
        removed_count = 0
        
        # Use temporary file for atomic operation
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
            temp_path = temp_file.name
            
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile:
                for line in infile:
                    line = line.strip()
                    if line and line not in entries_to_remove_set:
                        temp_file.write(line + '\n')
                    elif line in entries_to_remove_set:
                        removed_count += 1
        
        # Atomic move of temporary file to original location
        shutil.move(temp_path, file_path)
        
        logger.info(f"  Removed {removed_count:,} entries from {file_path.name}")
        return removed_count
        
    except Exception as e:
        logger.error(f"Error removing entries from {file_path}: {e}")
        # Clean up temp file if it exists
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        raise

def process_log_file(file_path, dry_run=False):
    """Process a single log file for safe data redistribution"""
    try:
        current_date = extract_date_from_filename(file_path.name)
        if current_date is None:
            logger.warning(f"Skipping file with unparseable date: {file_path.name}")
            return 0, 0, 0, 0, 0, 0
        
        prev_day_file = file_path.parent / f"logs_{(current_date - timedelta(days=1)).strftime('%Y-%m-%d')}.txt"
        next_day_file = file_path.parent / f"logs_{(current_date + timedelta(days=1)).strftime('%Y-%m-%d')}.txt"
        
        logger.info(f"Processing {file_path.name} -> redistributing to {prev_day_file.name} and {next_day_file.name}")
        
        # Read and categorize current file
        current_entries = []
        prev_day_entries = []
        next_day_entries = []
        current_day_records = 0
        prev_day_records = 0
        next_day_records = 0
        skipped_records = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile:
                for line_num, line in enumerate(infile, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        log_data = json.loads(line)
                        log_message = log_data.get('log', '')
                        
                        entry_timestamp = extract_timestamp_from_log_entry(log_message)
                        
                        if entry_timestamp is None:
                            current_entries.append(line)
                            skipped_records += 1
                            continue
                        
                        entry_date = entry_timestamp.date()
                        current_date_only = current_date.date()
                        
                        if entry_date == current_date_only:
                            current_entries.append(line)
                            current_day_records += 1
                        elif entry_date == (current_date + timedelta(days=1)).date():
                            next_day_entries.append(line)
                            next_day_records += 1
                        elif entry_date == (current_date - timedelta(days=1)).date():
                            prev_day_entries.append(line)
                            prev_day_records += 1
                        else:
                            current_entries.append(line)
                            skipped_records += 1
                            logger.warning(f"Line {line_num}: Unexpected date {entry_date} (expected around {current_date_only})")
                    
                    except json.JSONDecodeError:
                        current_entries.append(line)
                        skipped_records += 1
                    except Exception as e:
                        current_entries.append(line)
                        skipped_records += 1
                        logger.error(f"Line {line_num}: Error processing entry: {e}")
            
            # Step 1: Append entries to target files (safe operation)
            if prev_day_entries:
                append_entries_to_file(prev_day_file, prev_day_entries, dry_run)
            
            if next_day_entries:
                append_entries_to_file(next_day_file, next_day_entries, dry_run)
            
            # Step 2: Update current file with only current day entries (single pass)
            if not dry_run:
                # Use temporary file for atomic operation
                with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
                    temp_path = temp_file.name
                    
                    # Write only current day entries (this automatically excludes prev/next day entries)
                    for entry in current_entries:
                        temp_file.write(entry + '\n')
                
                # Atomic move to replace original file
                shutil.move(temp_path, file_path)
                logger.info(f"  Updated {file_path.name}: {len(current_entries):,} entries")
            else:
                logger.info(f"  Would update {file_path.name}: {len(current_entries):,} entries")
            
            logger.info(f"  Current day records: {current_day_records:,}")
            logger.info(f"  Previous day records: {prev_day_records:,}")
            logger.info(f"  Next day records: {next_day_records:,}")
            logger.info(f"  Skipped/other: {skipped_records:,}")
            logger.info(f"  Entries redistributed: {len(prev_day_entries + next_day_entries):,}")
            
            return len(current_entries), len(prev_day_entries), len(next_day_entries), current_day_records, prev_day_records, next_day_records
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return 0, 0, 0, 0, 0, 0
            
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return 0, 0, 0, 0, 0, 0

def process_log_directory(directory_path, dry_run=False, backup=False):
    """Process all log files in a directory for safe data redistribution"""
    directory = Path(directory_path)
    
    if not directory.exists() or not directory.is_dir():
        logger.error(f"Directory does not exist: {directory_path}")
        return
    
    log_files = list(directory.glob('logs_*.txt'))
    
    if not log_files:
        logger.warning(f"No log files found in {directory_path}")
        return
    
    # Sort files by date to ensure proper processing order
    log_files.sort(key=lambda x: extract_date_from_filename(x.name) or datetime.max)
    
    logger.info(f"Found {len(log_files)} log files to process")
    
    total_files = 0
    total_current_entries = 0
    total_prev_entries = 0
    total_next_entries = 0
    total_current_day_records = 0
    total_prev_day_records = 0
    total_next_day_records = 0
    total_skipped_records = 0
    
    for file_path in log_files:
        if file_path.name in ["logs_2025-03-01.txt", "logs_2025-03-02.txt", "logs_2025-03-03.txt"]:
            logger.info(f"Skipping empty file: {file_path.name}")
            continue
            
        current_entries, prev_entries, next_entries, current_records, prev_records, next_records = process_log_file(file_path, dry_run)
        
        if current_entries > 0 or prev_entries > 0 or next_entries > 0:
            total_files += 1
            total_current_entries += current_entries
            total_prev_entries += prev_entries
            total_next_entries += next_entries
            total_current_day_records += current_records
            total_prev_day_records += prev_records
            total_next_day_records += next_records
            
            # Calculate skipped records for this file
            file_skipped = current_entries - current_records
            total_skipped_records += file_skipped
    
    # Summary
    logger.info("=" * 60)
    logger.info("SAFE REDISTRIBUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Files processed: {total_files}")
    logger.info(f"Total current day entries: {total_current_entries:,}")
    logger.info(f"Total previous day entries: {total_prev_entries:,}")
    logger.info(f"Total next day entries: {total_next_entries:,}")
    logger.info(f"Total current day records: {total_current_day_records:,}")
    logger.info(f"Total previous day records: {total_prev_day_records:,}")
    logger.info(f"Total next day records: {total_next_day_records:,}")
    logger.info(f"Total skipped/other records: {total_skipped_records:,}")
    
    # Data quality warning if skipped records are high
    if total_skipped_records > 0:
        skipped_percentage = (total_skipped_records / total_current_entries) * 100 if total_current_entries > 0 else 0
        logger.warning(f"Data quality: {skipped_percentage:.2f}% of entries were skipped/other")
        if skipped_percentage > 5:
            logger.warning("High percentage of skipped records - consider investigating data quality issues")

def main():
    parser = argparse.ArgumentParser(description='Safely redistribute log data between daily log files')
    parser.add_argument('directory', help='Directory containing log files')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--backup', action='store_true', help='Create backup before processing')
    
    args = parser.parse_args()
    
    if args.backup:
        backup_dir = Path(args.directory) / 'backup_before_redistribution'
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        shutil.copytree(args.directory, backup_dir)
        logger.info(f"Created backup at: {backup_dir}")
    
    process_log_directory(args.directory, args.dry_run, args.backup)

if __name__ == "__main__":
    main()
