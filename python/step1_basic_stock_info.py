import pandas as pd
import requests
from datetime import datetime, timedelta, date
from io import StringIO
import re
from bs4 import BeautifulSoup
import ssl
from utils import get_root_path, get_dataframe_by_css_selector

# 發現是urlopen https時需要驗證一次SSL證書，當網站目標使用自簽名的證書時就會跳出這個錯誤
ssl._create_default_https_context = ssl._create_unverified_context

def get_daily_exchange_report(filter):
    """
    Fetch daily exchange report and filter based on criteria.
    filter 過濾條件：本益比 小於 10 且 殖利率 大於 3。
    """
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json"
    response = requests.get(url)
    data = response.json()["data"]
    
    columns = ["證券代號", "證券名稱", "收盤價", "殖利率(%)", "股利年度", "本益比", "股價淨值比", "財報年/季"]
    stock_df = pd.DataFrame(data, columns=columns)
    stock_df = stock_df.rename(columns={"殖利率(%)": "殖利率", "股價淨值比": "淨值比"})

    if filter:
        stock_df["淨值比"] = pd.to_numeric(stock_df["淨值比"], errors="coerce")
        stock_df["本益比"] = pd.to_numeric(stock_df["本益比"], errors="coerce")
        stock_df["殖利率"] = pd.to_numeric(stock_df["殖利率"].replace("-", 0), errors="coerce")
        candidate = stock_df[(stock_df["本益比"] < 10) & (stock_df["殖利率"] > 3)]
        return candidate

    return stock_df

def get_daily_exchange():
    """Fetch daily exchange data."""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/BFT41U?selectType=ALL&response=json"
    data = requests.get(url).json()
    df = pd.DataFrame(data["data"], columns=data["fields"])
    return df[["證券代號", "成交價"]]

def get_stock_capital(filter):
    """
    Fetch stock capital data and filter based on criteria.
    filter 過濾條件：上市日期早於五年前。
    """
    url = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
    df = pd.read_csv(url)

    if filter:
        five_years_ago = (datetime.today() - timedelta(days=5 * 365)).strftime("%Y%m%d")
        df = df[pd.to_datetime(df["上市日期"], format="%Y%m%d") < five_years_ago]

    df["實收資本額"] = pd.to_numeric(df["實收資本額"], downcast="float") / 100000000
    return df[["公司代號", "公司名稱", "實收資本額", "成立日期", "上市日期"]].rename(columns={"公司代號": "證券代號", "實收資本額": "資本額"})

def get_operating_margin():
    """Fetch operating margin data."""
    df = get_financial_statement('營益分析')
    df.columns = ["證券代號", "公司名稱", "營業收入", "毛利率", "營業利益率", "稅前純益率", "稅後純益率"]
    df["營業收入"] = pd.to_numeric(df["營業收入"], downcast="float") / 100
    return df.drop(columns=["公司名稱"])

def get_basic_stock_info(filter=False):
    """
    Fetch basic stock information and merge various data sources.
    filter 過濾條件：本益比 小於 10 且 殖利率 大於 3。
                    上市日期早於五年前。
    """
    exchange_report = get_daily_exchange_report(filter)
    capital = get_stock_capital(filter)
    
    # 確保證券代號的數據類型一致
    exchange_report["證券代號"] = exchange_report["證券代號"].astype(str)
    capital["證券代號"] = capital["證券代號"].astype(str)
    
    merge_df = pd.merge(capital, exchange_report, on="證券代號")

    if filter:
        operating_margin_df = get_operating_margin()
        operating_margin_df["證券代號"] = operating_margin_df["證券代號"].astype(str)
        merge_df = pd.merge(merge_df, operating_margin_df, on="證券代號")
        
        daily_exchange_df = get_daily_exchange()
        daily_exchange_df["證券代號"] = daily_exchange_df["證券代號"].astype(str)
        merge_df = pd.merge(merge_df, daily_exchange_df, on="證券代號")
        
        director_sharehold_df = get_director_sharehold()
        director_sharehold_df["證券代號"] = director_sharehold_df["證券代號"].astype(str)
        merge_df = pd.merge(merge_df, director_sharehold_df, on="證券代號")
        
        shareholder_distribution_df = get_all_shareholder_distribution()
        shareholder_distribution_df["證券代號"] = shareholder_distribution_df["證券代號"].astype(str)
        merge_df = pd.merge(merge_df, shareholder_distribution_df, on="證券代號")

    merge_df = merge_df[["證券代號", "證券名稱"] + [col for col in merge_df.columns if col not in ["證券代號", "證券名稱"]]]
    merge_df.to_csv(f"{get_root_path()}/Data/Temp/基本資訊.csv", encoding="utf_8_sig")
    return merge_df

def get_financial_statement(type='綜合損益'):
    """Fetch financial statement data."""
    now = date.today()
    current_year = now.year
    roc_year = current_year - 1911
    season = 0

    last_q4_day = date(current_year, 3, 31)
    q1_day = date(current_year, 5, 15)
    q2_day = date(current_year, 8, 14)
    q3_day = date(current_year, 11, 14)
    q4_day = date(current_year + 1, 3, 31)

    if now <= last_q4_day:
        roc_year -= 1
        season = 3
    elif now <= q1_day:
        roc_year -= 1
        season = 4
    elif now <= q2_day:
        season = 1
    elif now <= q3_day:
        season = 2
    elif now <= q4_day:
        season = 3

    url_map = {
        '綜合損益': 'https://mops.twse.com.tw/mops/web/ajax_t163sb04',
        '資產負債': 'https://mops.twse.com.tw/mops/web/ajax_t163sb05',
        '營益分析': 'https://mops.twse.com.tw/mops/web/ajax_t163sb06'
    }

    url = url_map.get(type)
    if not url:
        print('type does not match')
        return pd.DataFrame()

    form_data = {
        "encodeURIComponent": 1,
        "step": 1,
        "firstin": 1,
        "off": 1,
        "isQuery": "Y",
        "TYPEK": "sii",
        "year": roc_year,
        "season": str(season).zfill(2),
    }

    response = requests.post(url, form_data)
    response.encoding = "utf8"
    soup = BeautifulSoup(response.text, "html.parser")

    if not soup.find(string=re.compile("查詢無資料")):
        df_table = pd.read_html(StringIO(response.text))
        df = df_table[0].drop_duplicates(keep='first')
        df = df.rename(columns=df.iloc[0]).drop(df.index[0])
        for col in df.columns[2:]:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.rename(columns={"公司代號": "證券代號"})

    return pd.DataFrame()

def get_all_shareholder_distribution():
    """Fetch all shareholder distribution data."""
    url = 'https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5'
    df = pd.read_csv(url)

    df['key2'] = df.groupby('證券代號').cumcount() + 1
    s = df.set_index(['資料日期', '證券代號', 'key2']).unstack().sort_index(level=1, axis=1)
    s.columns = s.columns.map('{0[0]}_{0[1]}'.format)
    s = s.rename_axis([None], axis=1).reset_index()

    retail_headers = ['1-999', '1,000-5,000', '5,001-10,000', '10,001-15,000', '15,001-20,000', '20,001-30,000', '30,001-40,000', '40,001-50,000', '50,001-100,000']
    distribution_range_headers = retail_headers + ['100,001-200,000', '200,001-400,000', '400,001-600,000', '600,001-800,000', '800,001-1,000,000', '1,000,001', '差異數調整', '合計']

    new_title = ['資料日期', '證券代號'] + [distribution + title for distribution in distribution_range_headers for title in ['人數', '比例', '持股分級', '股數']]
    s.columns = new_title

    s['100張以下比例'] = s[[retail_header + '比例' for retail_header in retail_headers]].sum(axis=1)
    s['100張以下人數'] = s[[retail_header + '人數' for retail_header in retail_headers]].sum(axis=1)
    s = s.rename(columns={
        '100,001-200,000比例': '101-200張比例', '100,001-200,000人數': '101-200張人數',
        '200,001-400,000比例': '201-400張比例', '200,001-400,000人數': '201-400張人數',
        '400,001-600,000比例': '401-600張比例', '400,001-600,000人數': '401-600張人數',
        '600,001-800,000比例': '601-800張比例', '600,001-800,000人數': '601-800張人數',
        '800,001-1,000,000比例': '801-1000張比例', '800,001-1,000,000人數': '801-1000張人數',
        '1,000,001比例': '1000張以上比例', '1,000,001人數': '1000張以上人數'
    })
    s['401-800張人數'] = s[['401-600張人數', '601-800張人數']].sum(axis=1)
    s['401-800張比例'] = s[['401-600張比例', '601-800張比例']].sum(axis=1)

    return s[['證券代號', '100張以下人數', '100張以下比例', '101-200張人數', '101-200張比例', '201-400張人數', '201-400張比例', '401-800張人數', '401-800張比例', '801-1000張人數', '801-1000張比例', '1000張以上人數', '1000張以上比例']]

def get_director_sharehold():
    """Fetch director sharehold data."""
    url = "https://norway.twsthr.info/StockBoardTop.aspx"
    css_selector = "#details"
    df = get_dataframe_by_css_selector(url, css_selector)
    df.columns = df.columns.get_level_values(0)
    df = df.iloc[:, [3, 7]]
    df['證券代號'] = df['個股代號/名稱'].str[0:4]
    df = df.rename(columns={"持股比率 %": "全體董監持股(%)"})
    return df[['證券代號', '全體董監持股(%)']]

# Example usage
if __name__ == "__main__":
    df = get_basic_stock_info(True)
    df.to_csv(f"{get_root_path()}/content/basic_stock_info.csv", encoding="utf_8_sig")