import pandas as pd

df_mapping = pd.read_excel("mapping.xlsx")
df_source = pd.read_excel("listapi.xlsx")

def find_instansi(api_provider):
    if pd.isna(api_provider):
        return None
    if "@" in api_provider:
        for _, row in df_mapping.iterrows():
            domain = str(row["Domain"])
            if pd.notna(domain) and domain in api_provider:
                return row["Nama Instansi"]
    else:
        for _, row in df_mapping.iterrows():
            akun = str(row["Akun Nasional"])
            if pd.notna(akun) and akun in api_provider:
                return row["Nama Instansi"]
    return "Tidak Terdaftar"

df_source["Nama Instansi"] = df_source["api_provider"].apply(find_instansi)

output_path = "mapped_instansi.xlsx"
df_source.to_excel(output_path, index=False)
