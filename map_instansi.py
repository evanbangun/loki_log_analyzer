import pandas as pd
import re

df_mapping = pd.read_excel("mapping.xlsx")
df_source = pd.read_excel("listapi.xlsx")

def find_instansi(api_provider, api_name=None):
    if pd.isna(api_provider):
        return None
    if "@" in api_provider:
        for _, row in df_mapping.iterrows():
            domain = str(row["Domain"])
            if pd.notna(domain) and domain in api_provider:
                return row["Nama Instansi"]
    elif api_provider == "admin":
        api_name = str(api_name).lower()
        if "prov" in api_name:
            cleaned_str = re.sub(r'satudata|opendata|prov|data|open|-CSW|portal|-|_', '', api_name, flags=re.IGNORECASE)
            cleaned_str = cleaned_str.strip().replace(" ", "")
            for _, row in df_mapping.iterrows():
                nama_instansi = str(row["Nama Instansi"])
                if pd.notna(nama_instansi) and (cleaned_str in nama_instansi.lower().replace(" ", "") or cleaned_str in str(row["Akun Nasional"]).lower() or cleaned_str in str(row["Domain"]).lower()) and row["Tipe Instansi"] == "Provinsi":
                    print(api_name, row["Nama Instansi"])
                    return row["Nama Instansi"]
        elif "kab" in api_name:
            cleaned_str = re.sub(r'satudata|opendata|kab|data|open|-CSW|portal|-|_', '', api_name, flags=re.IGNORECASE)
            cleaned_str = cleaned_str.strip().replace(" ", "")
            for _, row in df_mapping.iterrows():
                nama_instansi = str(row["Nama Instansi"])
                if pd.notna(nama_instansi) and (cleaned_str in nama_instansi.lower().replace(" ", "") or cleaned_str in str(row["Akun Nasional"]).lower() or cleaned_str in str(row["Domain"]).lower()) and row["Tipe Instansi"] == "Kabupaten":
                    print(api_name, row["Nama Instansi"])
                    return row["Nama Instansi"]
        elif "kota" in api_name:
            cleaned_str = re.sub(r'satudata|opendata|kota|data|open|-CSW|portal|-|_', '', api_name, flags=re.IGNORECASE)
            cleaned_str = cleaned_str.strip().replace(" ", "")
            for _, row in df_mapping.iterrows():
                nama_instansi = str(row["Nama Instansi"])
                if pd.notna(nama_instansi) and (cleaned_str in nama_instansi.lower().replace(" ", "") or cleaned_str in str(row["Akun Nasional"]).lower() or cleaned_str in str(row["Domain"]).lower()) and row["Tipe Instansi"] == "Kota":
                    print(api_name, row["Nama Instansi"])
                    return row["Nama Instansi"]
        else:
            cleaned_str = re.sub(r'satudata|opendata|data|open|-CSW|portal|-|_', '', api_name, flags=re.IGNORECASE)
            cleaned_str = cleaned_str.strip().replace(" ", "")
            for _, row in df_mapping.iterrows():
                nama_instansi = str(row["Nama Instansi"])
                if pd.notna(nama_instansi) and (cleaned_str in nama_instansi.lower().replace(" ", "") or cleaned_str in str(row["Akun Nasional"]).lower() or cleaned_str in str(row["Domain"]).lower()):
                    print(api_name, row["Nama Instansi"])
                    return row["Nama Instansi"]
    else:
        for _, row in df_mapping.iterrows():
            akun = str(row["Akun Nasional"])
            if pd.notna(akun) and akun in api_provider:
                return row["Nama Instansi"]
    return "Tidak Terdaftar"

df_source["Nama Instansi"] = df_source.apply(lambda row: find_instansi(row["api_provider"], row["api_name"]), axis=1)

output_path = "mapped_instansi.xlsx"
df_source.to_excel(output_path, index=False)
