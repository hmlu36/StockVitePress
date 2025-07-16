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


def init():
    """初始化函式，設定各種環境"""
    set_utf8_encoding()
    ignore_ssl_warnings()


def set_utf8_encoding():
    sys.stdout.reconfigure(encoding="utf-8")


def ignore_ssl_warnings():
    """Ignore SSL warnings for insecure requests."""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def fetch_data(url):
    headers = get_headers(url)
    response = requests.get(url, headers=headers, timeout=30, verify=False)
    response.encoding = "utf-8"
    response.raise_for_status()
    return response


def post_data(url, data=None, json=None):
    headers = get_headers(url)
    response = requests.post(
        url, headers=headers, data=data, json=json, timeout=30, verify=False
    )
    response.raise_for_status()
    return response


def get_headers(url):
    ua = pyuser_agent.UA()
    parsed_url = urlparse(url)

    return {
        "User-Agent": ua.random,
        "Referer": f"{parsed_url.scheme}://{parsed_url.netloc}",
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

    response = fetch_data(url)
    # print(response.text)

    soup = BeautifulSoup(response.text, "html.parser")

    # 等待指定的時間以確保頁面加載完成
    time.sleep(wait_time)

    data = soup.select_one(css_selector)

    try:
        dfs = pd.read_html(StringIO(data.prettify()))
        
        for df in dfs:
            if len(df) > 1:
                return df
    except Exception as e:
        print(f"一般方法取得資料失敗: {e}，將使用 Selenium 嘗試...")


    # 使用 Selenium 方式抓取
    print(f"使用 Selenium 抓取資料: {url}")
    try:
        # 設定 Chrome 選項
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # 無頭模式
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        # 建立 WebDriver
        service = Service()
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # 存取網頁
        driver.get(url)

        # 等待指定的時間以確保頁面載入完成
        print(f"等待頁面載入 {wait_time} 秒...")
        time.sleep(wait_time)

        # 檢查頁面是否仍在初始化
        if "初始化中" in driver.page_source:
            # 如果還在初始化，再多等待一段時間
            print("頁面仍在初始化中，增加等待時間...")
            time.sleep(10)

        # 使用 CSS 選擇器查找元素
        try:
            element = driver.find_element(By.CSS_SELECTOR, css_selector)
            html_content = element.get_attribute("outerHTML")
        except Exception as e:
            print(f"在 Selenium 中找不到元素: {e}")
            driver.quit()
            return pd.DataFrame()

        # 解析 HTML 內容
        try:
            dfs = pd.read_html(StringIO(html_content))
            driver.quit()
            for df in dfs:
                if len(df) > 1:
                    return df
            return pd.DataFrame()
        except ValueError as e:
            print(f"解析 Selenium 獲取的 HTML 時發生錯誤: {e}")
            driver.quit()
            return pd.DataFrame()

    except Exception as e:
        print(f"Selenium 抓取過程中發生錯誤: {e}")
        try:
            driver.quit()
        except:
            pass
        return pd.DataFrame()


def get_business_day(count=1):
    end_date = datetime.today()
    start_date = end_date - relativedelta(months=1)  # 假設從一個月前開始
    business_days = pd.bdate_range(start=start_date, end=end_date)
    if count <= len(business_days):
        return business_days[-count]  # 取出往前第 count 個營業日
    else:
        raise ValueError("Count exceeds the number of business days in the range")


def format_number(value, decimal_places=2):
    """將數值格式化為指定小數位數，並移除末尾的 0，不足位數補空格"""
    # 處理 0 的特殊情況，直接返回靠右對齊的 0
    if value == 0:
        return '0'

    # 先格式化為指定小數位數
    formatted = f"{value:.{decimal_places}f}"

    # 計算小數點後的位數
    if '.' in formatted:
        decimal_part = formatted.split('.')[1]
        current_places = len(decimal_part)

        # 移除末尾的 0
        while formatted.endswith('0') and '.' in formatted:
            formatted = formatted[:-1]

        # 若小數點後沒有數字，則移除小數點
        if formatted.endswith('.'):
            formatted = formatted[:-1]
            current_places = -1
        else:
            # 重新計算移除末尾 0 後的位數
            if '.' in formatted:
                current_places = len(formatted.split('.')[1])
            else:
                current_places = 0

        # 補足空格以達到指定位數
        padding_spaces = ' ' * (decimal_places - current_places)
        formatted = formatted + padding_spaces

    return formatted

# 將 date_str 格式從 "114/07/10" 轉換為 "114年07月10日"


def format_date_to_chinese(date_str):
    """將民國日期格式 (114/07/10) 轉換為中文格式 (114年07月10日)"""
    parts = date_str.split('/')
    if len(parts) == 3:
        return f"{parts[0]}年{parts[1]}月{parts[2]}日"
    elif len(parts) == 2:
        return f"{parts[0]}年{parts[1]}月"
    else:
        return date_str


def convert_to_billion(value, decimal_places=2):
    """將數值轉換為億元單位"""
    BILLION = 100000000  # 億元單位轉換常數
    if isinstance(value, str):
        value = value.replace(",", "")
    return round(float(value) / BILLION, decimal_places)
