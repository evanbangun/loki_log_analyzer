import pandas as pd
import json
import pyarrow.parquet as pq
import os
import re
from datetime import datetime

def verify_order(ndjson_file, parquet_file):
    """Verify if the records are properly sorted by time."""
    print(f"Verifying time-based ordering for:\n{parquet_file}\n")
    
    # Read Parquet file
    parquet_df = pq.read_table(parquet_file).to_pandas()
    
    # Check if time column is sorted
    is_sorted = parquet_df['time'].is_monotonic_increasing
    
    if is_sorted:
        print("✅ Records are properly sorted by time!")
        
        # Show some sample timestamps
        print("\nSample timestamps:")
        print("First record:", parquet_df['time'].iloc[0])
        print("Last record:", parquet_df['time'].iloc[-1])
        print(f"Total records: {len(parquet_df)}")
        
        # Check for any duplicate timestamps
        duplicates = parquet_df[parquet_df['time'].duplicated(keep=False)]
        if not duplicates.empty:
            print(f"\nFound {len(duplicates)} records with duplicate timestamps")
            print("Sample of duplicate timestamps:")
            print(duplicates[['time', 'correlationId']].head())
    else:
        print("❌ Records are not properly sorted by time!")
        
        # Find where the order breaks
        for i in range(len(parquet_df) - 1):
            if parquet_df['time'].iloc[i] > parquet_df['time'].iloc[i + 1]:
                print(f"\nOrder breaks at index {i}:")
                print(f"Record {i}:", parquet_df['time'].iloc[i])
                print(f"Record {i+1}:", parquet_df['time'].iloc[i + 1])
                break

if __name__ == "__main__":
    # Example usage
    date = input("Enter date to verify (YYYY-MM-DD): ")
    parquet_file = f"E:/SPLP_Logs_parquet/day={date}/logs.parquet"
    
    if not os.path.exists(parquet_file):
        print(f"Error: Parquet file not found: {parquet_file}")
    else:
        verify_order(None, parquet_file) 