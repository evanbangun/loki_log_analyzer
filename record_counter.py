import os
import csv

# Set the directory to search for CSV files (current directory)
directory = 'F:/Processed Logs UNCLEAN/Bulan3'

# Find all CSV files in the directory
csv_files = [f for f in os.listdir(directory) if f.lower().endswith('.csv') and os.path.isfile(os.path.join(directory, f))]

if not csv_files:
    print('No CSV files found in the directory.')
else:
    total_rows = 0
    for csv_file in csv_files:
        file_path = os.path.join(directory, csv_file)
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                # Skip the header
                header = next(reader, None)
                # Count the remaining rows
                row_count = sum(1 for _ in reader)
            print(f'{csv_file}: {row_count} data rows')
            total_rows += row_count
        except Exception as e:
            print(f'Error reading {csv_file}: {e}')
    print(f'Total data rows across all CSV files: {total_rows}')
