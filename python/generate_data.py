import requests
import pandas as pd

def fetch_data():
    url = "https://www.cns11643.gov.tw/opendata/release.json"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    json_data = response.json()

    # Convert JSON data to DataFrame
    df = pd.DataFrame(json_data)

    # Save DataFrame to CSV
    df.to_csv('public/release.csv', index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    fetch_data()