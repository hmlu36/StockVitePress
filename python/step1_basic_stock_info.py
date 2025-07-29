"""
台股資料分析系統
整合多個資料來源，提供股票基本資訊分析功能
"""

import pandas as pd
from datetime import datetime, timedelta, date
from io import StringIO
from typing import Dict, List, Optional, Tuple
import utils

# 全域配置參數
TWSE_DAILY_REPORT_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json"
TWSE_DAILY_EXCHANGE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/BFT41U?selectType=ALL&response=json"
MOPS_CAPITAL_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
MOPS_API_URL = "https://mops.twse.com.tw/mops/api/redirectToOld"
TDCC_SHAREHOLDER_URL = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
DIRECTOR_SHAREHOLDER_URL = "https://norway.twsthr.info/StockBoardTop.aspx"
ROE_URL = "https://stock.wespai.com/p/10291"

# 篩選條件
PE_RATIO_THRESHOLD = 10.0        # 本益比上限
PE_RATIO_MIN_THRESHOLD = 0.0     # 本益比下限
YIELD_THRESHOLD = 8.0            # 殖利率下限
LISTING_YEARS_THRESHOLD = 5      # 上市年限下限

# 欄位配置
DAILY_REPORT_COLUMNS = [
    "證券代號", "證券名稱", "收盤價", "殖利率(%)",
    "股利年度", "本益比", "股價淨值比", "財報年/季"
]

COLUMN_RENAME_MAP = {
    "殖利率(%)": "殖利率",
    "股價淨值比": "淨值比",
    "公司代號": "證券代號",
    "實收資本額": "資本額"
}

# 財務報表類型對應
REPORT_TYPE_MAP = {
    "綜合損益": "t163sb04",
    "資產負債": "t163sb05",
    "營益分析": "t163sb06"
}

# 添加全域快取變數
_report_period_cache = None


def apply_filters(df, filters):
    """套用多個篩選條件"""
    result = df.copy()

    for column, condition in filters.items():
        if column in result.columns:
            if isinstance(condition, dict):
                if 'min' in condition:
                    result = result[result[column] >= condition['min']]
                if 'max' in condition:
                    result = result[result[column] <= condition['max']]
            elif callable(condition):
                result = result[condition(result[column])]

    return result


def filter_stock_code(df, column="證券代號"):
    """篩選出符合條件（四碼數字）的證券代號"""
    if column in df.columns:
        # 先去除空白
        df[column] = df[column].str.strip()
        # 篩選四碼數字的證券代號
        df = df[df[column].str.match(r'^\d{4}$')]
    return df


def get_daily_exchange_report(apply_filter=False):
    """獲取每日交易報告"""
    try:
        print("正在獲取每日交易報告...")
        response = utils.fetch_data(TWSE_DAILY_REPORT_URL)
        data = response.json().get("data", [])

        if not data:
            print("每日交易報告無資料")
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=DAILY_REPORT_COLUMNS)
        df = df.rename(columns=COLUMN_RENAME_MAP)

        if apply_filter:
            # 轉換數值欄位
            df = utils.convert_to_numeric(df, "本益比")
            df = utils.convert_to_numeric(df, "殖利率", {"-": "0"})
            df = utils.convert_to_numeric(df, "淨值比")

            # 套用篩選條件
            filters = {
                "本益比": {"max": PE_RATIO_THRESHOLD, "min": PE_RATIO_MIN_THRESHOLD},
                "淨值比": {"min": 0},
                "殖利率": {"min": YIELD_THRESHOLD}
            }
            df = apply_filters(df, filters)

        print(f"成功獲取 {len(df)} 筆每日交易資料")
        return df

    except Exception as e:
        print(TWSE_DAILY_REPORT_URL)
        print(f"獲取每日交易報告失敗: {e}")
        return pd.DataFrame()


def get_stock_capital(apply_filter=False):
    """獲取股本資料"""
    try:
        print("正在獲取股本資料...")
        response = utils.fetch_data(MOPS_CAPITAL_URL)
        response.encoding = "utf-8"
        df = pd.read_csv(StringIO(response.text))

        if apply_filter:
            cutoff_date = datetime.today() - timedelta(days=LISTING_YEARS_THRESHOLD * 365)
            cutoff_str = cutoff_date.strftime("%Y%m%d")
            df["上市日期"] = pd.to_datetime(df["上市日期"], format="%Y%m%d", errors='coerce')
            df = df[df["上市日期"] < cutoff_str]

        # 處理資本額（轉換為億元）
        df["實收資本額"] = df["實收資本額"].apply(lambda x: utils.convert_to_billion(x))

        result_columns = ["公司代號", "公司名稱", "實收資本額", "成立日期", "上市日期"]
        df = df[result_columns].rename(columns=COLUMN_RENAME_MAP)

        # 將日期欄位格式化為 yyyy/MM/dd
        for col in ["成立日期", "上市日期"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y/%m/%d")

        print(f"成功獲取 {len(df)} 筆股本資料")
        return df

    except Exception as e:
        print(f"獲取股本資料失敗: {e}")
        return pd.DataFrame()


def get_daily_exchange():
    """獲取盤後定價交易資料"""
    try:
        print("正在獲取盤後定價交易資料...")
        response = utils.fetch_data(TWSE_DAILY_EXCHANGE_URL)
        data = response.json()

        df = pd.DataFrame(data["data"], columns=data["fields"])
        result = df[["證券代號", "成交價"]]

        print(f"成功獲取 {len(result)} 筆盤後交易資料")
        return result

    except Exception as e:
        print(f"獲取盤後交易資料失敗: {e}")
        return pd.DataFrame()


def get_latest_report_period(today=None):
    """計算最新財報期間（添加快取機制）"""
    global _report_period_cache

    if today is None:
        today = date.today()

    # 如果是同一天且已有快取，直接返回
    if _report_period_cache and _report_period_cache[0] == today:
        return _report_period_cache[1]

    year = today.year
    roc_year = year - 1911

    # 財報公布截止日
    deadlines = {
        1: date(year, 5, 15),   # Q1
        2: date(year, 8, 14),   # Q2
        3: date(year, 11, 14),  # Q3
        4: date(year, 3, 31)    # Q4 (前一年)
    }

    if today <= deadlines[4]:
        return roc_year - 2, 3
    elif today <= deadlines[1]:
        return roc_year - 1, 4
    elif today <= deadlines[2]:
        return roc_year, 1
    elif today <= deadlines[3]:
        return roc_year, 2
    else:
        return roc_year, 3


def get_financial_statement(report_type="綜合損益", year=None, season=None):
    """獲取財務報表"""
    try:
        if year is None or season is None:
            year, season = get_latest_report_period()

        if not (1 <= season <= 4):
            raise ValueError("季度必須是 1, 2, 3, 4 之一")

        ajax_code = REPORT_TYPE_MAP.get(report_type)
        if not ajax_code:
            print(f"不支援的報表類型: {report_type}")
            return pd.DataFrame()

        # 構建請求參數
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

        # 發送請求
        response = utils.post_data(MOPS_API_URL, json=payload)
        api_response = response.json()
        final_url = api_response.get("result", {}).get("url")

        if not final_url:
            print("無法獲取財務報表 URL")
            return pd.DataFrame()

        # 獲取最終資料
        final_response = utils.fetch_data(final_url)

        if "查詢無資料" in final_response.text:
            print(f"查詢無資料：民國 {year} 年第 {season} 季 {report_type}")
            return pd.DataFrame()

        # 解析 HTML 表格
        df_list = pd.read_html(StringIO(final_response.text))
        if not df_list:
            print("找不到任何表格")
            return pd.DataFrame()

        df = df_list[0]
        df = df[df.iloc[:, 0] != df.columns[0]]  # 移除重複標頭
        df = df.rename(columns={"公司代號": "證券代號"})

        # 轉換數值欄位
        for col in df.columns[2:]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df.reset_index(drop=True)

    except Exception as e:
        print(f"獲取財務報表失敗: {e}")
        return pd.DataFrame()


def get_operating_margin():
    """獲取營業利益率資料"""
    try:
        print("正在獲取營業利益率資料...")
        df = get_financial_statement("營益分析")

        if df.empty:
            return df

        df.columns = [
            "證券代號", "公司名稱", "營業收入", "毛利率",
            "營業利益率", "稅前純益率", "稅後純益率"
        ]

        df["營業收入"] = (pd.to_numeric(df["營業收入"], errors='coerce') / 100).round(4)
        result = df.drop(columns=["公司名稱"])

        print(f"成功獲取 {len(result)} 筆營業利益率資料")
        return result

    except Exception as e:
        print(f"獲取營業利益率資料失敗: {e}")
        return pd.DataFrame()


def get_director_shareholders():
    """獲取董監持股資料"""
    try:
        print("正在獲取董監持股資料...")
        css_selector = "#details"
        df = utils.get_dataframe_by_css_selector(
            DIRECTOR_SHAREHOLDER_URL,
            css_selector,
            wait_time=0
        )

        if df.empty:
            print("董監持股資料為空")
            return df

        # 處理多層次欄位名稱 - 合併兩層 level 為一個欄位名稱
        if isinstance(df.columns, pd.MultiIndex):
            # 將 MultiIndex 的兩層合併成單一層
            new_columns = []
            for col in df.columns:
                # 排除空白或重複的 level 名稱
                if col[0] == col[1] or 'Unnamed' in col[0] or 'Unnamed' in col[1]:
                    new_columns.append(col[0] if 'Unnamed' not in col[0] else col[1])
                else:
                    # 合併兩個 level 的名稱
                    new_columns.append(f"{col[0]}_{col[1]}")

            df.columns = new_columns

        # 選取需要的欄位：個股代號/名稱 和 本月持股比率
        df = df[["個股代號/名稱", "類別", "持股比率 %_前二月", "持股比率 %_前一月", "持股比率 %_本 月"]].rename(
            columns={"個股代號/名稱": "證券代號",
                     "持股比率 %_前二月": "前二月董監持股%",
                     "持股比率 %_前一月": "前一月董監持股%",
                     "持股比率 %_本 月": "本月董監持股%"}
        )

        # 確保主要資料框架的證券代號只有前四碼
        df["證券代號"] = df["證券代號"].str[:4]

        print(f"成功獲取 {len(df)} 筆董監持股資料")
        return df

    except Exception as e:
        print(DIRECTOR_SHAREHOLDER_URL)
        print(f"獲取董監持股資料失敗: {e}")
        return pd.DataFrame()


def get_all_shareholder_distribution():
    """獲取股東分布資料"""
    print("正在獲取股東分布資料...")
    df = pd.read_csv(TDCC_SHAREHOLDER_URL)

    # 篩選四碼數字證券代號
    df = filter_stock_code(df)

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


def get_basic_stock_info(apply_filter=False):
    """獲取基本股票資訊"""
    try:
        print("開始分析股票資料...")

        # 獲取基礎資料
        exchange_report = get_daily_exchange_report(apply_filter)
        capital = get_stock_capital(apply_filter)

        if exchange_report.empty or capital.empty:
            print("基礎資料獲取失敗")
            return pd.DataFrame()

        # 確保證券代號型別一致
        exchange_report = utils.ensure_string_type(exchange_report, "證券代號")
        capital = utils.ensure_string_type(capital, "證券代號")

        # 合併基礎資料
        merged_df = pd.merge(capital, exchange_report, on="證券代號", how="inner")
        print(f"基礎資料合併完成，共 {len(merged_df)} 筆")

        # 如果需要篩選，則加入額外資料
        if apply_filter:
            # 定義額外資料來源
            additional_sources = [
                ("營業利益率", get_operating_margin),
                ("盤後交易", get_daily_exchange),
                ("董監持股", get_director_shareholders),
                ("股東分布", get_all_shareholder_distribution)
            ]

            for name, fetch_func in additional_sources:
                try:
                    print(f"正在處理: {name}")
                    additional_df = fetch_func()

                    if not additional_df.empty:
                        additional_df = utils.ensure_string_type(additional_df, "證券代號")
                        merged_df = pd.merge(merged_df, additional_df, on="證券代號", how="left")
                        print(f"{name} 資料合併完成")
                    else:
                        print(f"{name} 無可用資料")

                except Exception as e:
                    print(f"處理 {name} 時發生錯誤: {e}")

        # 重新排列欄位
        if not merged_df.empty:
            cols = ["證券代號", "證券名稱"] + [
                col for col in merged_df.columns
                if col not in ["證券代號", "證券名稱"]
            ]
            merged_df = merged_df[cols]

        print(f"分析完成，最終資料筆數: {len(merged_df)}")
        return merged_df

    except Exception as e:
        print(f"獲取基本股票資訊時發生錯誤: {e}")
        return pd.DataFrame()


def main():
    try:
        # 初始化
        utils.init()

        # 執行分析
        result_df = get_basic_stock_info(apply_filter=True)

        # 儲存結果
        if not result_df.empty:
            utils.save_to_csv(result_df)
            print(f"\n=== 分析完成 ===")
            print(f"符合條件的股票數量: {len(result_df)}")
            print(f"資料已儲存至: public/basic_stock_info.csv")

            # 顯示前5筆資料預覽
            if len(result_df) > 0:
                print(f"\n前5筆資料預覽:")
                print(result_df.head().to_string(index=False))
        else:
            print("查無符合條件的資料")

    except Exception as e:
        print(f"程式執行失敗: {e}")


if __name__ == "__main__":
    main()

    # 測試用程式碼
    # df = get_director_shareholders()
    # df = get_all_shareholder_distribution()
    # print(df)
