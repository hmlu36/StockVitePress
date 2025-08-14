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
from playwright.sync_api import sync_playwright
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import urllib3
import sys
from pathlib import Path


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

    with sync_playwright() as playwright:
        try:
            # 設定瀏覽器選項
            browser = playwright.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']  # 避免被偵測為自動化
            )
            ua = pyuser_agent.UA()
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=ua.random
            )
            page = context.new_page()

            # 設定更長的超時時間，並改用 domcontentloaded 而不是 networkidle
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)  # 60秒超時
                # 額外等待確保動態內容載入
                page.wait_for_timeout(5000)  # 等待 5 秒
                
                # 先取得頁面內容再檢查
                content = page.content()
                #print(content)
                
                if "初始化中" in content:
                    # 如果還在初始化，再多等待一段時間
                    print("頁面仍在初始化中，增加等待時間...")
                    page.wait_for_timeout(10000)  # 多等待 10 秒
            except Exception as goto_error:
                print(f"頁面載入失敗，嘗試重新載入: {goto_error}")
                try:
                    # 嘗試用 load 策略重新載入
                    page.goto(url, wait_until="load", timeout=60000)
                    page.wait_for_timeout(3000)
                except Exception as retry_error:
                    print(f"重新載入也失敗: {retry_error}")
                    browser.close()
                    return pd.DataFrame()


            # 使用 CSS 選擇器查找元素
            try:
                # 等待元素出現
                element = page.wait_for_selector(css_selector, timeout=30000)
                if not element:
                    print(f"在 Playwright 中找不到元素: {css_selector}")
                    browser.close()
                    return pd.DataFrame()

                # 獲取元素的 HTML
                html_content = element.inner_html()

                # 解析 HTML 內容
                try:
                    soup = BeautifulSoup(f"<table>{html_content}</table>", "html.parser")
                    dfs = pd.read_html(StringIO(soup.prettify()))

                    browser.close()

                    for df in dfs:
                        if len(df) > 1:
                            return df
                    return pd.DataFrame()
                except Exception as e:
                    print(f"解析 Playwright 獲取的 HTML 時發生錯誤: {e}")
                    browser.close()
                    return pd.DataFrame()

            except Exception as e:
                print(f"在 Playwright 中處理元素時發生錯誤: {e}")
                browser.close()
                return pd.DataFrame()

        except Exception as e:
            print(f"Playwright 抓取過程中發生錯誤: {e}")
            try:
                browser.close()
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


def save_to_csv(df: pd.DataFrame, filename: str = "basic_stock_info.csv"):
    """儲存資料到 CSV"""
    try:
        output_dir = Path("public")
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / filename

        df.to_csv(output_path, encoding="utf-8-sig", index=False)
        print(f"資料已成功輸出至: {output_path}")
        print(f"共處理 {len(df)} 筆資料")

    except Exception as e:
        print(f"儲存檔案失敗: {e}")


def convert_to_numeric(df, column, replace_values=None, fill_value=0):
    """將欄位轉換為數值型別"""
    if column not in df.columns:
        return df

    series = df[column].copy()

    if replace_values:
        for old_val, new_val in replace_values.items():
            series = series.replace(old_val, new_val)

    df[column] = pd.to_numeric(series, errors='coerce').fillna(fill_value)
    return df


def ensure_string_type(df, column):
    """確保指定欄位為字串型別"""
    if column in df.columns:
        df[column] = df[column].astype(str)
    return df
