import pandas as pd
import numpy as np

def match_and_update_data():
    # # 1st Step
    # output_grouped = pd.read_excel('output_grouped_National.xlsx')
    # mapped_instansi = pd.read_excel('mapped_instansi_cleaned.xlsx')

    # 2nd Step
    output_grouped = pd.read_excel('output_grouped_Internal.xlsx')
    mapped_instansi = pd.read_excel('mapped_instansi_updated.xlsx')

    if 'Occurrence' not in mapped_instansi.columns:
        mapped_instansi['Occurrence'] = np.nan

    for _, row in output_grouped.iterrows():
        base_conditions = (mapped_instansi['api_name'] == row['apiName']) & \
                         (mapped_instansi['created_by'] == row['apiCreator'])
        
        if row['apiCreatorTenantDomain'] == 'carbon.super':
            mask = base_conditions & (mapped_instansi['domain'] == 'nasional')
        else:
            mask = base_conditions & (mapped_instansi['domain'] == row['apiCreatorTenantDomain'])
        
        if mask.any():
            mapped_instansi.loc[mask, 'Occurrence'] = row['Occurrence']
        else:
            new_row = {
                'api_provider': row['apiCreator'],
                'created_by': row['apiCreator'],
                'api_name': row['apiName'],
                'api_version': None,
                'domain': 'nasional' if row['apiCreatorTenantDomain'] == 'carbon.super' else row['apiCreatorTenantDomain'],
                'status': 'PUBLISHED',
                'Nama Instansi': row['Instansi Pemilik API'],
                'Occurrence': row['Occurrence']
            }
            mapped_instansi = pd.concat([mapped_instansi, pd.DataFrame([new_row])], ignore_index=True)

    mapped_instansi.to_excel('mapped_instansi_updated.xlsx', index=False)

if __name__ == "__main__":
    match_and_update_data()
