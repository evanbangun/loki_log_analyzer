import pandas as pd
import os
import glob
import sys
from datetime import datetime

def validate_date_format(date_str):
    """Validate date format YYYY-MM-DD"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def parse_date_range(date_range):
    """Parse date range in format YYYY-MM-DD//YYYY-MM-DD"""
    if '//' not in date_range:
        print("Error: Date range must be in format YYYY-MM-DD//YYYY-MM-DD")
        return None, None
    
    start_date, end_date = date_range.split('//')
    
    if not validate_date_format(start_date) or not validate_date_format(end_date):
        print("Error: Invalid date format. Use YYYY-MM-DD format.")
        return None, None
    
    return start_date, end_date

def filter_by_date_range(df, start_date, end_date):
    """Filter dataframe by date range"""
    # Convert log_timestamp to datetime for comparison
    df['date'] = pd.to_datetime(df['log_timestamp'])
    
    # Filter by date range
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    filtered_df = df[mask].copy()
    
    # Remove the temporary date column
    filtered_df = filtered_df.drop('date', axis=1)
    
    return filtered_df

def get_user_choice():
    """Get user choice for processing mode"""
    print("\n" + "="*50)
    print("LOKI LOG ANALYZER - PROCESSING OPTIONS")
    print("="*50)
    print("1. Process All Data")
    print("2. Process Date Range")
    print("3. Exit")
    print("="*50)
    
    while True:
        try:
            choice = input("Enter your choice (1-3): ").strip()
            if choice in ['1', '2', '3']:
                return choice
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
        except KeyboardInterrupt:
            print("\nExiting...")
            sys.exit(0)

def get_date_range_input():
    """Get date range input from user"""
    while True:
        try:
            date_range = input("Enter date range (YYYY-MM-DD//YYYY-MM-DD): ").strip()
            start_date, end_date = parse_date_range(date_range)
            if start_date and end_date:
                return start_date, end_date
            else:
                print("Invalid date format. Please try again.")
        except KeyboardInterrupt:
            print("\nExiting...")
            sys.exit(0)

def filter_csv_files_by_date_range(csv_files, start_date, end_date):
    """Pre-filter CSV files by date range based on filename"""
    relevant_files = []
    
    for file in csv_files:
        # Extract date from filename (assuming format: logs_YYYY-MM-DD.csv)
        filename = os.path.basename(file)
        if filename.startswith('logs_') and filename.endswith('.csv'):
            try:
                # Extract date part from filename
                date_str = filename[5:-4]  # Remove 'logs_' prefix and '.csv' suffix
                file_date = pd.to_datetime(date_str)
                
                # Check if file date is within range
                if start_date <= file_date <= end_date:
                    relevant_files.append(file)
                    print(f'  Including file: {filename} (Date: {date_str})')
                else:
                    print(f'  Skipping file: {filename} (Date: {date_str} - outside range)')
            except ValueError:
                # If filename doesn't match expected format, include it for safety
                relevant_files.append(file)
                print(f'  Including file: {filename} (Unknown date format)')
        else:
            # If filename doesn't match expected format, include it for safety
            relevant_files.append(file)
            print(f'  Including file: {filename} (Non-standard filename)')
    
    return relevant_files

# Get user choice for processing mode
choice = get_user_choice()

if choice == '3':
    print("Exiting...")
    sys.exit(0)

start_date = None
end_date = None
use_date_filter = False

if choice == '2':
    start_date, end_date = get_date_range_input()
    # Convert to datetime objects for comparison
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    use_date_filter = True
    print(f'Processing logs from {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
else:
    print('Processing all available data...')

folder_path = 'E:/Processed Logs'
print('Finding all CSV files in ' + folder_path + ' ...')
all_csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

if not all_csv_files:
    print('No CSV files found in the folder.')
    exit(1)

print(f'Found {len(all_csv_files)} total CSV files.')

# Pre-filter files by date range if specified
if use_date_filter:
    print(f'Pre-filtering files for date range: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
    csv_files = filter_csv_files_by_date_range(all_csv_files, start_date, end_date)
    print(f'Found {len(csv_files)} relevant files for the specified date range.')
else:
    csv_files = all_csv_files
    print(f'Processing all {len(csv_files)} files (no date filtering).')

if not csv_files:
    print('No relevant CSV files found for the specified date range.')
    exit(1)

print('Processing and grouping relevant files...')
columns_to_keep = ['log_timestamp', 'api_name', 'api_creator','api_creator_tenant_domain', 'application_owner', 'application_name']

print('Loading mapping Excel file...')
mapping_df = pd.read_excel('mapping.xlsx')
mapping_df['Akun Nasional'] = mapping_df['Akun Nasional'].astype(str)
mapping_df['Domain'] = mapping_df['Domain'].astype(str)

def find_nama_instansi(api_creator):
    match = mapping_df[mapping_df['Akun Nasional'] == str(api_creator)]
    if not match.empty:
        return match.iloc[0]['Nama Instansi']
    for _, row in mapping_df.iterrows():
        if row['Domain'] in str(api_creator):
            return row['Nama Instansi']
    return None

def find_nama_instansi_requester(application_owner):
    # Exact match
    match = mapping_df[mapping_df['Akun Nasional'] == str(application_owner)]
    if not match.empty:
        return match.iloc[0]['Nama Instansi']
    # Substring match
    for _, row in mapping_df.iterrows():
        if str(application_owner) in row['Akun Nasional'] or row['Domain'] in str(application_owner):
            return row['Nama Instansi']
    return None

all_grouped = []
for file in csv_files:
    print(f'  Loading and grouping {file}...')
    try:
        df = pd.read_csv(file, usecols=columns_to_keep)
        # Save original timestamps for comparison
        original_timestamps = df['log_timestamp'].copy()
        # Clean up timestamps before extracting date
        df['log_timestamp'] = df['log_timestamp'].astype(str).str.strip()
        # Extract date by splitting at the first space
        df['log_timestamp'] = df['log_timestamp'].str.split(' ').str[0]
        # Validate date format without changing the column
        parsed_dates = pd.to_datetime(df['log_timestamp'], errors='coerce')
        if parsed_dates.isna().any():
            unparsable_values = df.loc[parsed_dates.isna(), 'log_timestamp'].unique()
            print(f'Error: Found unparsable date values in {file}: {[repr(x) for x in unparsable_values]}')
            print('Script stopped. Please fix the data format.')
            exit(1)
        
        # Filter by date range if specified
        if use_date_filter:
            df_filtered = filter_by_date_range(df, start_date, end_date)
            if not df_filtered.empty:
                # Process api_creator_tenant_domain column
                df_filtered['api_creator_tenant_domain_processed'] = df_filtered['api_creator_tenant_domain'].apply(
                    lambda x: "Nasional" if str(x) == 'carbon.super' else str(x)
                )
                
                grouped = df_filtered.groupby(['log_timestamp', 'api_name', 'api_creator', 'api_creator_tenant_domain_processed', 'application_owner', 'application_name']).size().reset_index(name='occurrence')
                # Rename the processed column back to original name
                grouped = grouped.rename(columns={'api_creator_tenant_domain_processed': 'api_creator_tenant_domain'})
                # Map Nama Instansi here for each file's grouped data
                grouped['Nama Instansi'] = grouped['api_creator'].apply(find_nama_instansi)
                all_grouped.append(grouped)
            else:
                print(f'    No data found in date range for {file}')
        else:
            # Process all data without filtering
            # Process api_creator_tenant_domain column
            df['api_creator_tenant_domain_processed'] = df['api_creator_tenant_domain'].apply(
                lambda x: "Nasional" if str(x) == 'carbon.super' else str(x)
            )
            
            grouped = df.groupby(['log_timestamp', 'api_name', 'api_creator', 'api_creator_tenant_domain_processed', 'application_owner', 'application_name']).size().reset_index(name='occurrence')
            # Rename the processed column back to original name
            grouped = grouped.rename(columns={'api_creator_tenant_domain_processed': 'api_creator_tenant_domain'})
            # Map Nama Instansi here for each file's grouped data
            grouped['Nama Instansi'] = grouped['api_creator'].apply(find_nama_instansi)
            all_grouped.append(grouped)
    except Exception as e:
        print(f'    Skipping {file} due to error: {e}')

if not all_grouped:
    print('No valid CSV files loaded.')
    exit(1)

print('Concatenating all grouped results...')
all_grouped_df = pd.concat(all_grouped, ignore_index=True)

print('Final grouping and summing occurrences...')
final_grouped = all_grouped_df.groupby(
    ['log_timestamp', 'api_name', 'api_creator', 'api_creator_tenant_domain', 'application_owner', 'application_name'],
    as_index=False
).agg({
    'occurrence': 'sum',
    'Nama Instansi': 'first'
})

# Rename 'Nama Instansi' to 'Nama Instansi Pemilik API'
final_grouped = final_grouped.rename(columns={'Nama Instansi': 'Nama Instansi Pemilik API'})

# Add 'Nama Instansi Requester' by mapping application_owner to mapping.Akun Nasional with both exact and substring rules
final_grouped['Nama Instansi Requester'] = final_grouped['application_owner'].apply(find_nama_instansi_requester)
final_grouped['Nama Instansi Requester'] = final_grouped['Nama Instansi Requester'].fillna('Tidak Terdaftar')
final_grouped['Nama Instansi Requester'].replace('', 'Tidak Terdaftar', inplace=True)

# Reorder columns: log_timestamp, Nama Instansi Pemilik API, api_name, api_creator, api_creator_tenant_domain, Nama Instansi Requester, application_owner, application_name, occurrence
new_order = [
    'log_timestamp',
    'Nama Instansi Pemilik API',
    'api_name',
    'api_creator',
    'api_creator_tenant_domain',
    'Nama Instansi Requester',
    'application_owner',
    'application_name',
    'occurrence'
]
final_grouped = final_grouped[new_order]

# Create output filename based on processing mode
if use_date_filter:
    output_filename = f'Report_By_Date_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.csv'
else:
    output_filename = 'Report_By_Date_All_Data.csv'

print(f'Saving mapped result to {output_filename}...')
final_grouped.to_csv(output_filename, index=False)
print('Done!')