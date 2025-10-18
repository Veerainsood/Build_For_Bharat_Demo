from dataHandlers.fetchers.data_fetcher import DataFetcher
from intelligence.analyzers.data_analyzer import DataAnalyzer


def analyze_rainfall_crop_relation(rain_url, crop_url):
    f = DataFetcher()
    rain_df = f.load(rain_url)
    crop_df = f.load(crop_url)

    rain = DataAnalyzer(rain_df)
    crop = DataAnalyzer(crop_df)

    merged = rain.join(crop, on=["STATE", "YEAR"])
    corr = merged.correlation("RAINFALL", "CROP_PRODUCTION")
    trend = merged.trend("CROP_PRODUCTION", "YEAR")

    return {
        "correlation": corr,
        "trend": trend.to_dict(orient="records")
    }

rain_url = "https://data.gov.in/files/ogdpv2dms/s3fs-public/RF_AI_1901-2021.csv"
crop_url = "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24?api-key=579b464db66ec23bdd000001cdc3b564546246a772a26393094f5645&offset=0&limit=all&format=csv"

correlation , trend = analyze_rainfall_crop_relation(rain_url, crop_url)

print("Correlation between Rainfall and Crop Production:", correlation)
print("Trend of Crop Production over Years:")
for record in trend:
    print(record)