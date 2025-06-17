import pandas as pd

df = pd.read_excel("Report/recap__National.xlsx")

grouped_df = df.groupby(["Instansi Pemilik API", "apiCreator", "apiName", "Instansi API Requester", "applicationOwner", "applicationName"], as_index=False)["Occurrence"].sum()

grouped_df.to_excel("output_grouped.xlsx", index=False)

