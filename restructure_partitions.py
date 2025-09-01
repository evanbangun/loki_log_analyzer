#!/usr/bin/env python3
"""
Script to restructure SPLP_Logs_parquet from flat day-based partitioning
to hierarchical year/month/day partitioning.

Current structure: SPLP_Logs_parquet/day=2025-07-31/logs.parquet
New structure: SPLP_Logs_parquet/year=2025/month=7/day=31/logs.parquet
"""

import os
import shutil
from pathlib import Path
from datetime import datetime
import argparse


def parse_date_from_dirname(dirname):
    """Extract date from directory name like 'day=2025-07-31'"""
    if dirname.startswith('day='):
        date_str = dirname[4:]  # Remove 'day=' prefix
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return None
    return None


def create_hierarchical_structure(base_path, date_obj):
    """Create year/month/day directory structure"""
    year_dir = base_path / f"year={date_obj.year}"
    month_dir = year_dir / f"month={date_obj.month}"
    day_dir = month_dir / f"day={date_obj.day}"
    
    # Create directories if they don't exist
    day_dir.mkdir(parents=True, exist_ok=True)
    
    return day_dir


def restructure_partitions(source_dir, dry_run=True):
    """
    Restructure parquet partitions from flat day-based to hierarchical year/month/day
    
    Args:
        source_dir: Path to the SPLP_Logs_parquet directory
        dry_run: If True, only show what would be done without making changes
    """
    source_path = Path(source_dir)
    
    if not source_path.exists():
        print(f"Error: Source directory {source_dir} does not exist")
        return
    
    # Get all day directories
    day_dirs = [d for d in source_path.iterdir() if d.is_dir() and d.name.startswith('day=')]
    
    if not day_dirs:
        print(f"No day-based directories found in {source_dir}")
        return
    
    print(f"Found {len(day_dirs)} day-based directories to restructure")
    
    if dry_run:
        print("\n=== DRY RUN MODE - No changes will be made ===")
    
    # Process each day directory
    for day_dir in sorted(day_dirs):
        date_obj = parse_date_from_dirname(day_dir.name)
        if not date_obj:
            print(f"Warning: Could not parse date from {day_dir.name}, skipping")
            continue
        
        # Check if logs.parquet exists
        parquet_file = day_dir / "logs.parquet"
        if not parquet_file.exists():
            print(f"Warning: No logs.parquet found in {day_dir.name}, skipping")
            continue
        
        # Create new hierarchical path
        new_day_dir = create_hierarchical_structure(source_path, date_obj)
        new_parquet_file = new_day_dir / "logs.parquet"
        
        print(f"\n{day_dir.name} -> year={date_obj.year}/month={date_obj.month}/day={date_obj.day}")
        print(f"  Source: {parquet_file}")
        print(f"  Target: {new_parquet_file}")
        
        if not dry_run:
            try:
                # Move the parquet file
                shutil.move(str(parquet_file), str(new_parquet_file))
                
                # Remove the old empty directory
                day_dir.rmdir()
                
                print(f"  ✓ Moved successfully")
            except Exception as e:
                print(f"  ✗ Error moving file: {e}")
        else:
            print(f"  [DRY RUN] Would move file")
    
    if dry_run:
        print("\n=== DRY RUN COMPLETE ===")
        print("To actually perform the restructuring, run with --execute flag")
    else:
        print("\n=== RESTRUCTURING COMPLETE ===")
        print("All parquet files have been moved to the new hierarchical structure")


def main():
    parser = argparse.ArgumentParser(
        description="Restructure SPLP_Logs_parquet from flat to hierarchical partitioning"
    )
    parser.add_argument(
        "source_dir",
        help="Path to the SPLP_Logs_parquet directory"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the restructuring (default is dry-run)"
    )
    
    args = parser.parse_args()
    
    print("SPLP Logs Parquet Partition Restructuring Tool")
    print("=" * 50)
    print(f"Source directory: {args.source_dir}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print()
    
    # Confirm before executing
    if args.execute:
        response = input("Are you sure you want to restructure the partitions? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Operation cancelled.")
            return
    
    restructure_partitions(args.source_dir, dry_run=not args.execute)


if __name__ == "__main__":
    main()
