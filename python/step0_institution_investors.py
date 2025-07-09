import pandas as pd
import requests
from datetime import datetime
from utils import get_headers, sleep, get_business_day, init, fetch_data
from pathlib import Path


def fetch_exchange_data(date):
    """獲取交易資料"""
    url = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date.strftime('%Y%m%d')}"
    response = fetch_data(url)
    return response.json()


def process_exchange_data(json_data):
    """處理交易資料"""
    df = pd.DataFrame(json_data["data"], columns=json_data["fields"])
    df = df[["日期", "成交金額"]]
    df["成交金額"] = pd.to_numeric(df["成交金額"].str.replace(",", ""))
    return df.rename(columns={"成交金額": "總成交金額"}).set_index("日期").T


def get_daily_exchange_amount(day_count=1):
    """獲取每日交易金額"""
    result = pd.DataFrame()

    for i in range(1, day_count + 1):
        try:
            date = get_business_day(i)
            data = fetch_exchange_data(date)
            df = process_exchange_data(data)
            result = pd.concat([result, df], axis=1) if not result.empty else df

            if day_count > 1:
                sleep()
        except:
            continue

    return result.sort_values(by="日期", axis=1, ascending=False).iloc[:, :day_count]


def fetch_investors_data(date):
    """獲取法人資料"""
    url = f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date.strftime('%Y%m%d')}&type=day"
    headers = get_headers(url)
    headers.update({"Referer": "https://www.twse.com.tw/"})

    response = requests.get(url, headers=headers, verify=False, timeout=30)
    return response.json()


def process_investors_data(json_data, amount_df, date_str):
    """處理法人資料"""
    df = pd.DataFrame(json_data["data"], columns=json_data["fields"])
    df["買賣差額"] = pd.to_numeric(df["買賣差額"].str.replace(",", ""))

    # 計算法人總成交金額（使用索引4，即"合計"行）
    institutional_total = (
        pd.to_numeric(df.loc[4, "買進金額"].replace(",", "")) +
        pd.to_numeric(df.loc[4, "賣出金額"].replace(",", ""))
    ) / 2

    # 基本資料
    result = df[["單位名稱", "買賣差額"]].copy()

    # 添加市場總交易金額
    market_total = amount_df.loc["總成交金額", date_str]
    result = pd.concat([result, pd.DataFrame([
        {"單位名稱": "市場總交易金額", "買賣差額": market_total},
        {"單位名稱": "法人成交比重", "買賣差額": round(institutional_total / market_total * 100, 2)}
    ])], ignore_index=True)

    # 轉換為億元
    result["買賣差額"] = (result["買賣差額"] / 100000000).round(3)

    return result.rename(columns={"單位名稱": "項目", "買賣差額": date_str})


def get_institutional_investors_exchange(day_count=1):
    """Fetch institutional investors exchange data for a given number of days."""
    amount_df = get_daily_exchange_amount(day_count)
    sum_df = pd.DataFrame()
    count = 0

    while sum_df.shape[1] < day_count:
        temp_date = datetime.today() - pd.tseries.offsets.BDay(count)
        mingo_date_str = str(temp_date.year - 1911) + "/" + temp_date.strftime("%m/%d")
        json_data = fetch_investors_data(temp_date)

        if json_data["stat"] == "OK":
            df = process_investors_data(json_data, amount_df, mingo_date_str)
            sum_df = pd.merge(sum_df, df, on=["項目"]) if not sum_df.empty else df

        count += 1
        if day_count > 1:
            sleep()  # 增加延遲時間

    sum_df = sum_df.set_index("項目")
    return sum_df


def main():
    """主程式"""
    init()
    df = get_institutional_investors_exchange(1)

    if not df.empty:
        print(df)
        Path("public").mkdir(exist_ok=True)
        df.to_csv("public/institutional_investors_exchange.csv", encoding="utf-8-sig")
        print("資料已儲存至 public/institutional_investors_exchange.csv")
    else:
        print("無法獲取資料")


if __name__ == "__main__":
    main()
