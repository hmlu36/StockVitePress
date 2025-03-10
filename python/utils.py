import os
import time
import random
import requests
import pyuser_agent
from urllib.parse import urlparse
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

def get_headers(url):
    """Generate random user-agent headers."""
    ua = pyuser_agent.UA()
    user_agent = ua.random
    parsed_url = urlparse(url)
    referer = f"{parsed_url.scheme}://{parsed_url.netloc}"
    headers = {
        "User-Agent": user_agent,
        "Referer": referer
    }
    return headers

def sleep():
    """Sleep for a random interval between 3 and 10 seconds."""
    time.sleep(random.randint(3, 10))

def get_root_path():
    """Get the root directory path of the current script."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_dataframe_by_css_selector(url, css_selector, wait_time=5):
    """
    Fetch HTML content from a URL, parse it using a CSS selector, and return a DataFrame.

    Parameters:
    url (str): The URL to fetch the HTML content from.
    css_selector (str): The CSS selector to locate the desired HTML element.

    Returns:
    pd.DataFrame: The parsed data as a DataFrame.
    """
    headers = get_headers(url)
    
    try:
        response = requests.get(url, headers=headers, timeout=(5, 10))
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return pd.DataFrame()

    response.encoding = "utf-8"
    print(response.text)
    soup = BeautifulSoup(response.text, "html.parser")
    
    
    # 等待指定的時間以確保頁面加載完成
    time.sleep(wait_time)
    
    data = soup.select_one(css_selector)
    if not data:
        print(f"No data found for CSS selector: {css_selector}")
        return pd.DataFrame()

    try:
        dfs = pd.read_html(StringIO(data.prettify()))
    except ValueError as e:
        print(f"Error parsing HTML to DataFrame: {e}")
        return pd.DataFrame()

    for df in dfs:
        if len(df) > 1:
            return df

    return pd.DataFrame()

# Example usage
url = "https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=IS_M_QUAR_ACC&STOCK_ID=8150"
css_selector = "#txtFinBody"
df = get_dataframe_by_css_selector(url, css_selector)
print(df)

def get_business_day(count=1):
    end_date = datetime.today()
    start_date = end_date - relativedelta(months=1)  # 假設從一個月前開始
    business_days = pd.bdate_range(start=start_date, end=end_date)
    if count <= len(business_days):
        return business_days[-count]  # 取出往前第 count 個營業日
    else:
        raise ValueError("Count exceeds the number of business days in the range")