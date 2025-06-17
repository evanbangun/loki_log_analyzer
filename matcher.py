import pandas as pd
import numpy as np

def match_and_update_data():
    # Read the Excel files
    output_grouped = pd.read_excel('output_grouped_temp.xlsx')
    mapped_instansi = pd.read_excel('mapped_instansi_temp.xlsx')

    # Create a new column for Occurrence if it doesn't exist
    if 'Occurrence' not in mapped_instansi.columns:
        mapped_instansi['Occurrence'] = np.nan

    # Iterate through each row in output_grouped
    for _, row in output_grouped.iterrows():
        # Find matching rows in mapped_instansi
        mask = (mapped_instansi['api_name'] == row['apiName']) & \
               (mapped_instansi['created_by'] == row['apiCreator'])
        
        if mask.any():
            # Update Occurrence for matching rows
            mapped_instansi.loc[mask, 'Occurrence'] = row['Occurrence']
        else:
            # Create new row
            new_row = {
                'api_provider': row['apiCreator'],
                'created_by': row['apiCreator'],
                'api_name': row['apiName'],
                'api_version': None,
                'domain': None,
                'status': 'PUBLISHED',
                'Nama Instansi': row['Instansi Pemilik API'],
                'Occurrence': row['Occurrence']
            }
            mapped_instansi = pd.concat([mapped_instansi, pd.DataFrame([new_row])], ignore_index=True)

    # Save the updated mapped_instansi to a new Excel file
    mapped_instansi.to_excel('mapped_instansi_updated.xlsx', index=False)
    print("Data matching and updating completed. Results saved to 'mapped_instansi_updated.xlsx'")

if __name__ == "__main__":
    match_and_update_data()
