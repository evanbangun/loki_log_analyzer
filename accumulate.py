import pandas as pd

df = pd.read_excel("Report/recap__Internal_Aggregated.xlsx")

grouped_df = df.groupby(["Instansi Pemilik API", "apiCreator", "apiName", "apiCreatorTenantDomain"], as_index=False)["Occurrence"].sum()

grouped_df.to_excel("output_grouped_Internal.xlsx", index=False)

