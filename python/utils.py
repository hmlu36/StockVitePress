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
import urllib3
import sys
import ssl


def init():
    """Initialize the environment by setting UTF-8 encoding and ignoring SSL warnings."""
    set_utf8_encoding()
    ignore_ssl_warnings()


from requests.adapters import HTTPAdapter


# --- 自訂 SSL Context 的輔助類別 ---
class CustomHttpAdapter(HTTPAdapter):
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = requests.packages.urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context,
        )


def get_session_with_custom_ssl():
    """建立一個使用自訂 SSL 安全等級的 requests.Session"""
    # 建立一個 SSL Context，並將安全等級設定為 1
    # 預設等級 2 非常嚴格，不允許缺少 SKI 的憑證
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    ctx.set_ciphers("ALL:@SECLEVEL=1")

    session = requests.Session()
    adapter = CustomHttpAdapter(ssl_context=ctx)
    session.mount("https://", adapter)
    return session


def fetch_data(url):
    headers = get_headers(url)
    response = requests.get(url, headers=headers, timeout=30, verify=False)
    response.raise_for_status()
    return response


def post_url(url, data=None, json=None):
    headers = get_headers(url)
    return requests.post(
        url, headers=headers, data=data, json=json, timeout=30, verify=False
    )


def set_utf8_encoding():
    sys.stdout.reconfigure(encoding="utf-8")


def ignore_ssl_warnings():
    """Ignore SSL warnings for insecure requests."""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_headers(url):
    ua = pyuser_agent.UA()
    parsed_url = urlparse(url)

    return {
        "User-Agent": ua.random,
        "Referer": f"{parsed_url.scheme}://{parsed_url.netloc}"
    }


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


def get_business_day(count=1):
    end_date = datetime.today()
    start_date = end_date - relativedelta(months=1)  # 假設從一個月前開始
    business_days = pd.bdate_range(start=start_date, end=end_date)
    if count <= len(business_days):
        return business_days[-count]  # 取出往前第 count 個營業日
    else:
        raise ValueError("Count exceeds the number of business days in the range")
