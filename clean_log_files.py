#!/usr/bin/env python3
"""
Log File Cleaner

This script cleans daily log files by removing log entries that don't match
the expected date in the filename. It processes JSON log entries and filters
out entries with timestamps that don't correspond to the file's date.
"""

import json
import re
import os
import argparse
from datetime import datetime
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_date_from_filename(filename):
    """
    Extract date from filename like 'logs_2025-07-29.txt'
    Returns datetime object or None if parsing fails
    """
    try:
        # Extract date part from filename
        match = re.search(r'logs_(\d{4}-\d{2}-\d{2})\.txt', filename)
        if match:
            date_str = match.group(1)
            return datetime.strptime(date_str, '%Y-%m-%d')
        return None
    except Exception as e:
        logger.error(f"Failed to parse date from filename {filename}: {e}")
        return None

def extract_timestamp_from_log_entry(log_entry):
    """
    Extract timestamp from log entry like '[2025-07-29 00:06:00,382]'
    Returns datetime object or None if parsing fails
    """
    try:
        # Extract timestamp from the beginning of log message
        match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\]', log_entry)
        if match:
            timestamp_str = match.group(1)
            return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        return None
    except Exception as e:
        logger.error(f"Failed to parse timestamp from log entry: {e}")
        return None

def clean_log_file(file_path, expected_date, dry_run=False):
    """
    Clean a single log file by removing entries that don't match the expected date
    
    Args:
        file_path: Path to the log file
        expected_date: Expected date as datetime object
        dry_run: If True, only count entries without modifying the file
    
    Returns:
        tuple: (total_entries, kept_entries, removed_entries)
    """
    total_entries = 0
    kept_entries = 0
    removed_entries = 0
    
    # Create temporary file for output
    temp_file_path = str(file_path) + '.tmp'
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile, \
             open(temp_file_path, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line:
                    continue
                
                total_entries += 1
                
                try:
                    # Parse JSON entry
                    log_data = json.loads(line)
                    log_message = log_data.get('log', '')
                    
                    # Extract timestamp from log message
                    entry_timestamp = extract_timestamp_from_log_entry(log_message)
                    
                    if entry_timestamp is None:
                        # If we can't parse timestamp, keep the entry (better safe than sorry)
                        logger.warning(f"Line {line_num}: Could not parse timestamp, keeping entry")
                        outfile.write(line + '\n')
                        kept_entries += 1
                        continue
                    
                    # Check if timestamp matches expected date
                    if entry_timestamp.date() == expected_date.date():
                        outfile.write(line + '\n')
                        kept_entries += 1
                    else:
                        removed_entries += 1
                        if not dry_run:
                            logger.debug(f"Line {line_num}: Removing entry with date {entry_timestamp.date()}")
                
                except json.JSONDecodeError:
                    # If line is not valid JSON, keep it (might be header/footer)
                    logger.warning(f"Line {line_num}: Invalid JSON, keeping as-is")
                    outfile.write(line + '\n')
                    kept_entries += 1
                except Exception as e:
                    # If any other error occurs, keep the entry
                    logger.error(f"Line {line_num}: Error processing entry: {e}, keeping as-is")
                    outfile.write(line + '\n')
                    kept_entries += 1
        
        # Replace original file with cleaned version
        if not dry_run:
            os.replace(temp_file_path, file_path)
            logger.info(f"Cleaned file: {file_path}")
        else:
            os.remove(temp_file_path)
            logger.info(f"Dry run completed for: {file_path}")
        
        return total_entries, kept_entries, removed_entries
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        # Clean up temp file if it exists
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        return 0, 0, 0

def process_log_directory(directory_path, dry_run=False, backup=False):
    """
    Process all log files in a directory
    
    Args:
        directory_path: Path to directory containing log files
        dry_run: If True, only count entries without modifying files
        backup: If True, create backup copies before cleaning
    """
    directory = Path(directory_path)
    
    if not directory.exists() or not directory.is_dir():
        logger.error(f"Directory does not exist: {directory_path}")
        return
    
    # Find all log files
    log_files = list(directory.glob('logs_*.txt'))
    
    if not log_files:
        logger.warning(f"No log files found in {directory_path}")
        return
    
    logger.info(f"Found {len(log_files)} log files to process")
    
    total_files = 0
    total_entries = 0
    total_kept = 0
    total_removed = 0
    
    for log_file in sorted(log_files):
        logger.info(f"Processing: {log_file.name}")
        
        # Extract expected date from filename
        expected_date = extract_date_from_filename(log_file.name)
        if expected_date is None:
            logger.error(f"Skipping {log_file.name}: Could not parse date from filename")
            continue
        
        # Create backup if requested
        if backup and not dry_run:
            backup_path = log_file.with_suffix('.txt.backup')
            try:
                import shutil
                shutil.copy2(log_file, backup_path)
                logger.info(f"Created backup: {backup_path}")
            except Exception as e:
                logger.error(f"Failed to create backup for {log_file.name}: {e}")
                continue
        
        # Clean the file
        entries, kept, removed = clean_log_file(log_file, expected_date, dry_run)
        
        total_files += 1
        total_entries += entries
        total_kept += kept
        total_removed += removed
        
        logger.info(f"  Total entries: {entries:,}")
        logger.info(f"  Kept entries: {kept:,}")
        logger.info(f"  Removed entries: {removed:,}")
        logger.info(f"  Removal rate: {(removed/entries*100):.2f}%" if entries > 0 else "  Removal rate: 0%")
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*50)
    logger.info(f"Files processed: {total_files}")
    logger.info(f"Total entries: {total_entries:,}")
    logger.info(f"Total kept: {total_kept:,}")
    logger.info(f"Total removed: {total_removed:,}")
    if total_entries > 0:
        logger.info(f"Overall removal rate: {(total_removed/total_entries*100):.2f}%")
    logger.info("="*50)

def main():
    parser = argparse.ArgumentParser(description='Clean daily log files by removing entries with wrong dates')
    parser.add_argument('directory', help='Directory containing log files')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without modifying files')
    parser.add_argument('--backup', action='store_true', help='Create backup copies before cleaning')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.dry_run:
        logger.info("DRY RUN MODE: No files will be modified")
    
    if args.backup:
        logger.info("BACKUP MODE: Backup copies will be created before cleaning")
    
    process_log_directory(args.directory, args.dry_run, args.backup)

if __name__ == '__main__':
    main()
