import os
import requests
import pandas as pd

def fetch_data():
    url = "https://www.cns11643.gov.tw/opendata/release.json"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    json_data = response.json()

    # Convert JSON data to DataFrame
    df = pd.DataFrame(json_data)
    
    # Ensure the public directory exists
    public_dir = 'public'
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    # Save DataFrame to CSV
    output_path = os.path.join('public', 'release.csv')
    df.to_csv(output_path, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    fetch_data()