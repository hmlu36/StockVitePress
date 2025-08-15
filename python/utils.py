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


# ...existing code...
def get_dataframe_by_css_selector(url, css_selector, wait_time=5, retries=3, headless=True, timeout=60000):
    """
    使用 requests 先嘗試取得靜態內容，若失敗或頁面需要 JS，改用 Playwright 抓取後解析成 DataFrame。
    參數:
      url (str): 目標網址
      css_selector (str): 要抓取的 CSS 選擇器
      wait_time (int): 在頁面載入後額外等待的秒數
      retries (int): Playwright 重試次數
      headless (bool): 是否無頭模式
      timeout (int): Playwright 導覽超時 (毫秒)
    回傳:
      pd.DataFrame
    """
    # 先用 requests 嘗試（較輕量）
    try:
        resp = fetch_data(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        el = soup.select_one(css_selector)
        if el:
            try:
                dfs = pd.read_html(StringIO(str(el)))
                for df in dfs:
                    if len(df) > 0:
                        return df
            except Exception:
                # 若靜態解析失敗，將改用 Playwright
                pass
    except Exception as e:
        # 網路或解析錯誤，改用 Playwright
        print(f"requests 取得頁面失敗: {e}")

    # 使用 Playwright 抓取（重試機制）
    last_err = None
    for attempt in range(1, retries + 1):
        with sync_playwright() as playwright:
            browser = None
            try:
                ua = pyuser_agent.UA()
                browser = playwright.chromium.launch(
                    headless=headless,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-blink-features=AutomationControlled"
                    ]
                )
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent=ua.random
                )
                page = context.new_page()
                # 導覽
                page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                # 等待到元素或額外等待時間
                try:
                    page.wait_for_selector(css_selector, timeout=min(timeout, 30000))
                except Exception:
                    # 若無法在短時間內找到，仍等候額外時間
                    page.wait_for_timeout(wait_time * 1000)

                # 取得元素 HTML（若找不到元素則取整頁）
                try:
                    element = page.query_selector(css_selector)
                    html = element.inner_html() if element else page.content()
                except Exception:
                    html = page.content()

                # 解析為 DataFrame
                soup = BeautifulSoup(html, "html.parser")
                # 如果選到的元素不是 <table>，嘗試直接找到 table
                tables = soup.find_all("table")
                if not tables:
                    # 若直接為片段且不是 table，嘗試把片段包成 table
                    try:
                        dfs = pd.read_html(StringIO(str(soup)))
                    except Exception:
                        dfs = []
                else:
                    dfs = []
                    for t in tables:
                        try:
                            dlist = pd.read_html(StringIO(str(t)))
                            dfs.extend(dlist)
                        except Exception:
                            continue

                # 關閉 page/context/browser
                try:
                    page.close()
                except:
                    pass
                try:
                    context.close()
                except:
                    pass
                try:
                    browser.close()
                except:
                    pass

                # 回傳第一個有效的 DataFrame
                for df in dfs:
                    if isinstance(df, pd.DataFrame) and len(df) > 0:
                        return df
                # 若沒有表格則回傳空的 DataFrame
                return pd.DataFrame()

            except Exception as e:
                last_err = e
                print(f"Playwright 嘗試第 {attempt} 次失敗: {e}")
                try:
                    if browser:
                        browser.close()
                except:
                    pass
                # 指數退避 (簡單)
                time.sleep(min(5 * attempt, 30))
                continue

    # 若全部重試都失敗，記錄錯誤並回傳空 DataFrame
    print(f"Playwright 全部重試失敗, 最後錯誤: {last_err}")
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
