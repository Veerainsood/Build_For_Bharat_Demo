from dataHandlers.fetchers.data_fetcher import DataFetcher

fetcher = DataFetcher()

# # CSV example
# df_csv = fetcher.load(
#     "https://data.gov.in/files/ogdpv2dms/s3fs-public/RF_AI_1901-2021.csv",
#     "csv_static"
# )

# # JSON API example
# df_json = fetcher.load(
#     "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24",
#     "json_api"
# )

# import requests
# import pandas as pd

# url = "https://exlink.pmkisan.gov.in/services/GovDataDetails.asmx/GetPMKIsanDatagov?FinYearID=6&TrimesterNo=2&StateCode=21&Distric=344&TokenNo=FHGBHFYBT268Gpf37hmJ6RY"

# r = requests.get(url)
# r.raise_for_status()

# data = r.json()      # direct parse to dict
# df = pd.DataFrame(data["Table"])

# print(df.head())
# print(df.shape)
df1 = fetcher.load_pmkisan_family("Nicobars:2022-2023[11th]")
