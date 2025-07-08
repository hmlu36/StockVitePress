import pandas as pd
import requests
from datetime import datetime, timedelta, date
from io import StringIO
import re
from bs4 import BeautifulSoup
import ssl
from utils import get_headers, get_dataframe_by_css_selector, init, fetch_data, post_url
import os
import json

init()


def get_daily_exchange_report(filter):
    """
    Fetch daily exchange report and filter based on criteria.
    filter 過濾條件：本益比 小於 10 且 殖利率 大於 3。
    """
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json"
    response = fetch_data(url)
    data = response.json()["data"]

    columns = [
        "證券代號",
        "證券名稱",
        "收盤價",
        "殖利率(%)",
        "股利年度",
        "本益比",
        "股價淨值比",
        "財報年/季",
    ]
    stock_df = pd.DataFrame(data, columns=columns)
    stock_df = stock_df.rename(columns={"殖利率(%)": "殖利率", "股價淨值比": "淨值比"})

    if filter:
        stock_df["淨值比"] = pd.to_numeric(stock_df["淨值比"], errors="coerce")
        stock_df["本益比"] = pd.to_numeric(stock_df["本益比"], errors="coerce")
        stock_df["殖利率"] = pd.to_numeric(
            stock_df["殖利率"].replace("-", 0), errors="coerce"
        )
        candidate = stock_df[(stock_df["本益比"] < 10) & (stock_df["殖利率"] > 3)]
        return candidate

    return stock_df


def get_daily_exchange():
    """Fetch daily exchange data."""
    url = "https://www.twse.com.tw/rwd/zh/afterTrading/BFT41U?selectType=ALL&response=json"
    data = fetch_data(url).json()
    df = pd.DataFrame(data["data"], columns=data["fields"])
    return df[["證券代號", "成交價"]]


def get_stock_capital(filter):
    """
    Fetch stock capital data and filter based on criteria.
    filter 過濾條件：上市日期早於五年前。
    """
    url = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
    response = fetch_data(url)
    response.encoding = "utf-8"
    df = pd.read_csv(StringIO(response.text))

    if filter:
        five_years_ago = (datetime.today() - timedelta(days=5 * 365)).strftime("%Y%m%d")
        df = df[pd.to_datetime(df["上市日期"], format="%Y%m%d") < five_years_ago]

    df["實收資本額"] = pd.to_numeric(df["實收資本額"], downcast="float") / 100000000
    return df[["公司代號", "公司名稱", "實收資本額", "成立日期", "上市日期"]].rename(
        columns={"公司代號": "證券代號", "實收資本額": "資本額"}
    )


def get_operating_margin():
    """Fetch operating margin data."""
    df = get_financial_statement("營益分析")

    if df.empty:
        return df
    df.columns = [
        "證券代號",
        "公司名稱",
        "營業收入",
        "毛利率",
        "營業利益率",
        "稅前純益率",
        "稅後純益率",
    ]
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
        director_sharehold_df["證券代號"] = director_sharehold_df["證券代號"].astype(
            str
        )
        merge_df = pd.merge(merge_df, director_sharehold_df, on="證券代號")

        shareholder_distribution_df = get_all_shareholder_distribution()
        shareholder_distribution_df["證券代號"] = shareholder_distribution_df[
            "證券代號"
        ].astype(str)
        merge_df = pd.merge(merge_df, shareholder_distribution_df, on="證券代號")

    merge_df = merge_df[
        ["證券代號", "證券名稱"]
        + [col for col in merge_df.columns if col not in ["證券代號", "證券名稱"]]
    ]
    return merge_df


def get_financial_statement(
    report_type: str = "綜合損益", year: int = None, season: int = None
):
    """
    從公開資訊觀測站獲取上市公司的合併財務報表。

    Args:
        year (int): 民國年 (e.g., 112 for 2023).
        season (int): 季度 (1, 2, 3, 4).
        report_type (str): 報表類型，可為 "綜合損益", "資產負債", "營益分析".

    Returns:
        pd.DataFrame: 包含財務報表資料的 DataFrame，若無資料則返回空的 DataFrame。
    """
    if year is None or season is None:
        # 如果沒有提供年份或季度，則自動計算最新的財報
        year, season = get_latest_report_period()

    if not (1 <= season <= 4):
        raise ValueError("季度 (season) 必須是 1, 2, 3, 4 之一。")

    # 報表類型與其對應的 ajax 代碼
    report_map = {
        "綜合損益": "t163sb04",
        "資產負債": "t163sb05",
        "營益分析": "t163sb06",
    }

    ajax_code = report_map.get(report_type)

    if not ajax_code:
        print(f"錯誤：報表類型 '{report_type}' 不存在。")
        return pd.DataFrame()

    # --- 第一步：取得加密參數 ---
    # 準備第一步請求的 payload
    payload = {
        "apiName": f"ajax_{ajax_code}",
        "parameters": {
            "year": str(year),
            "season": str(season).zfill(2),
            "TYPEK": "sii",
            "isQuery": "Y",
            "firstin": 1,
            "off": 1,
            "step": 1,
            "encodeURIComponent": 1,
        },
    }

    # --- 第一步：發送 POST 請求，獲取動態 URL ---
    first_step_url = "https://mops.twse.com.tw/mops/api/redirectToOld"

    # 發送 POST 請求以獲取加密參數
    headers = get_headers(first_step_url)
    response_step1 = post_url(first_step_url, json=payload)
    response_step1.raise_for_status()

    # 解析回傳的 JSON，取得加密字串
    api_response = response_step1.json()
    final_url = api_response.get("result", {}).get("url")
    print(f"final_url: {final_url}")

    # 發送 GET 請求
    response_final = fetch_data(final_url)
    response_final.raise_for_status()
    response_final.encoding = "utf8"

    if "查詢無資料" in response_final.text:
        print(f"查詢無資料：民國 {year} 年第 {season} 季 {report_type}。")
        return pd.DataFrame()

    # 使用 pandas 讀取 HTML 表格
    df_list = pd.read_html(StringIO(response_final.text))
    if not df_list:
        print("找不到任何表格。")
        return pd.DataFrame()

    # 資料清理 (與舊版相同)
    df = df_list[0]
    # 移除重複的標頭列，這在新版頁面中很常見
    df = df[df.iloc[:, 0] != df.columns[0]]
    df = df.rename(columns={"公司代號": "證券代號"})

    for col in df.columns[2:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.reset_index(drop=True)


def get_latest_report_period(today: date = date.today()):
    """
    根據今天的日期，計算出已公布的最新一季財報的年份和季度。
    財報公布期限:
    Q1: 5/15
    Q2: 8/14
    Q3: 11/14
    Q4: 隔年 3/31
    """
    year = today.year
    roc_year = year - 1911

    # 各季度財報公布截止日
    q1_deadline = date(year, 5, 15)
    q2_deadline = date(year, 8, 14)
    q3_deadline = date(year, 11, 14)
    q4_deadline = date(year, 3, 31)  # 前一年的 Q4

    if today <= q4_deadline:
        # 在 3/31 前，最新的是前前一年的 Q3
        return roc_year - 2, 3
    elif today <= q1_deadline:
        # 在 5/15 前，最新的是前一年的 Q4
        return roc_year - 1, 4
    elif today <= q2_deadline:
        # 在 8/14 前，最新的是當年度的 Q1
        return roc_year, 1
    elif today <= q3_deadline:
        # 在 11/14 前，最新的是當年度的 Q2
        return roc_year, 2
    else:  # 11/14 之後
        # 最新的是當年度的 Q3
        return roc_year, 3


def get_all_shareholder_distribution():
    """Fetch all shareholder distribution data."""
    url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
    df = pd.read_csv(url)

    df["key2"] = df.groupby("證券代號").cumcount() + 1
    s = (
        df.set_index(["資料日期", "證券代號", "key2"])
        .unstack()
        .sort_index(level=1, axis=1)
    )
    s.columns = s.columns.map("{0[0]}_{0[1]}".format)
    s = s.rename_axis([None], axis=1).reset_index()

    retail_headers = [
        "1-999",
        "1,000-5,000",
        "5,001-10,000",
        "10,001-15,000",
        "15,001-20,000",
        "20,001-30,000",
        "30,001-40,000",
        "40,001-50,000",
        "50,001-100,000",
    ]
    distribution_range_headers = retail_headers + [
        "100,001-200,000",
        "200,001-400,000",
        "400,001-600,000",
        "600,001-800,000",
        "800,001-1,000,000",
        "1,000,001",
        "差異數調整",
        "合計",
    ]

    new_title = ["資料日期", "證券代號"] + [
        distribution + title
        for distribution in distribution_range_headers
        for title in ["人數", "比例", "持股分級", "股數"]
    ]
    s.columns = new_title

    s["100張以下比例"] = s[
        [retail_header + "比例" for retail_header in retail_headers]
    ].sum(axis=1)
    s["100張以下人數"] = s[
        [retail_header + "人數" for retail_header in retail_headers]
    ].sum(axis=1)
    s = s.rename(
        columns={
            "100,001-200,000比例": "101-200張比例",
            "100,001-200,000人數": "101-200張人數",
            "200,001-400,000比例": "201-400張比例",
            "200,001-400,000人數": "201-400張人數",
            "400,001-600,000比例": "401-600張比例",
            "400,001-600,000人數": "401-600張人數",
            "600,001-800,000比例": "601-800張比例",
            "600,001-800,000人數": "601-800張人數",
            "800,001-1,000,000比例": "801-1000張比例",
            "800,001-1,000,000人數": "801-1000張人數",
            "1,000,001比例": "1000張以上比例",
            "1,000,001人數": "1000張以上人數",
        }
    )
    s["401-800張人數"] = s[["401-600張人數", "601-800張人數"]].sum(axis=1)
    s["401-800張比例"] = s[["401-600張比例", "601-800張比例"]].sum(axis=1)

    return s[
        [
            "證券代號",
            "100張以下人數",
            "100張以下比例",
            "101-200張人數",
            "101-200張比例",
            "201-400張人數",
            "201-400張比例",
            "401-800張人數",
            "401-800張比例",
            "801-1000張人數",
            "801-1000張比例",
            "1000張以上人數",
            "1000張以上比例",
        ]
    ]


def get_director_sharehold():
    """Fetch director sharehold data."""
    url = "https://norway.twsthr.info/StockBoardTop.aspx"
    css_selector = "#details"
    df = get_dataframe_by_css_selector(url, css_selector)
    df.columns = df.columns.get_level_values(0)
    df = df.iloc[:, [3, 7]]
    df["證券代號"] = df["個股代號/名稱"].str[0:4]
    df = df.rename(columns={"持股比率 %": "全體董監持股(%)"})
    return df[["證券代號", "全體董監持股(%)"]]


if __name__ == "__main__":

    # Ensure the public directory exists
    public_dir = "public"
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    df = get_basic_stock_info(True)
    output_path = os.path.join("public", "basic_stock_info.csv")
    df.to_csv(output_path, encoding="utf-8-sig", index=False)
