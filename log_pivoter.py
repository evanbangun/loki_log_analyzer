import pandas as pd
import os
import glob

folder_path = 'Processed Logs'
print('Finding all CSV files in ' + folder_path + ' ...')
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
if not csv_files:
    print('No CSV files found in the folder.')
    exit(1)

print(f'Found {len(csv_files)} files. Processing and grouping each file...')
columns_to_keep = ['log_timestamp', 'api_name', 'api_creator', 'application_owner', 'application_name']

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
        if str(application_owner) in row['Akun Nasional'] or row['Akun Nasional'] in str(application_owner):
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
        
        grouped = df.groupby(['log_timestamp', 'api_name', 'api_creator', 'application_owner', 'application_name']).size().reset_index(name='occurrence')
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
    ['log_timestamp', 'api_name', 'api_creator', 'application_owner', 'application_name'],
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

# Reorder columns: log_timestamp, Nama Instansi Pemilik API, api_name, api_creator, Nama Instansi Requester, application_owner, application_name, occurrence
new_order = [
    'log_timestamp',
    'Nama Instansi Pemilik API',
    'api_name',
    'api_creator',
    'Nama Instansi Requester',
    'application_owner',
    'application_name',
    'occurrence'
]
final_grouped = final_grouped[new_order]

print('Saving mapped result to Report_By_Date.csv...')
final_grouped.to_csv('Report_By_Date.csv', index=False)
print('Done!')