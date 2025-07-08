import pandas as pd
import requests
from datetime import datetime
import os
from utils import get_headers, sleep, get_business_day, init, fetch_data

init()


def fetch_exchange_data(date):
    """Fetch exchange data for a specific date."""
    url = f"https://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={date.strftime('%Y%m%d')}"
    response = fetch_data(url)

    # 檢查回應是否為空
    if not response.text.strip():
        raise ValueError(f"伺服器回傳空內容，日期: {date}")

    # 檢查是否為 HTML 錯誤頁面
    if response.text.strip().startswith(
        "<!DOCTYPE"
    ) or response.text.strip().startswith("<html"):
        print("收到 HTML 回應而非 JSON:")
        print(response.text[:500])
        raise ValueError("伺服器回傳 HTML 而非 JSON")

    try:
        return response.json()
    except Exception as e:
        print(f"JSON 解析失敗: {str(e)}")
        print(f"完整回應內容: {response.text}")
        raise e

def process_exchange_data(json_data):
    """Process the JSON data into a DataFrame."""
    df = pd.DataFrame(json_data["data"], columns=json_data["fields"])
    df = df[["日期", "成交金額"]]
    df["成交金額"] = pd.to_numeric(df["成交金額"].str.strip().str.replace(",", ""))
    df = df.rename(columns={"成交金額": "總成交金額"})
    return df.set_index("日期").T


def get_daily_exchange_amount(day_count=1):
    """Fetch daily exchange amount for a given number of days."""
    sum_df = pd.DataFrame()
    count = 1

    while sum_df.shape[1] < day_count:
        temp_date = get_business_day(count)
        json_data = fetch_exchange_data(temp_date)
        # print(json_data)
        df = process_exchange_data(json_data)

        sum_df = pd.concat([sum_df, df], axis=1) if not sum_df.empty else df
        count += 1
        if day_count > 1:
            sleep()  # 增加延遲時間

    sum_df = sum_df.sort_values(by="日期", axis=1, ascending=False)
    return sum_df.iloc[:, :day_count]


def fetch_investors_data(date):
    """Fetch institutional investors data for a specific date."""
    url = f"https://www.twse.com.tw/fund/BFI82U?response=json&dayDate={date.strftime('%Y%m%d')}&type=day"
    headers = get_headers(url)
    headers.update({"Referer": "https://www.twse.com.tw/"})
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()


def process_investors_data(json_data, amount_df, mingo_date_str):
    """Process the JSON data into a DataFrame."""
    df = pd.DataFrame(json_data["data"], columns=json_data["fields"])
    df["買賣差額"] = pd.to_numeric(df["買賣差額"].str.strip().str.replace(",", ""))
    total = (
        pd.to_numeric(df.loc[5, "買進金額"].replace(",", ""))
        + pd.to_numeric(df.loc[5, "賣出金額"].replace(",", ""))
    ) / 2

    df = df[["單位名稱", "買賣差額"]]
    temp_df = pd.DataFrame(
        [
            {
                "單位名稱": "市場總交易金額",
                "買賣差額": amount_df.loc["總成交金額", mingo_date_str],
            }
        ]
    )
    df = pd.concat([df, temp_df], axis=0, ignore_index=True)

    df["買賣差額"] = (
        pd.to_numeric(df["買賣差額"], downcast="float") / 100000000
    ).round(3)
    temp_df = pd.DataFrame(
        [
            {
                "單位名稱": "法人成交比重",
                "買賣差額": (
                    total / amount_df.loc["總成交金額", mingo_date_str] * 100
                ).round(2),
            }
        ]
    )
    df = pd.concat([df, temp_df], axis=0, ignore_index=True)

    df = df.rename(columns={"單位名稱": "項目", "買賣差額": mingo_date_str})
    return df


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


if __name__ == "__main__":
    df = get_institutional_investors_exchange(1)
    print(df)
    # Ensure the public directory exists
    public_dir = "public"
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    # Save DataFrame to CSV
    output_path = os.path.join("public", "institutional_investors_exchange.csv")
    df.to_csv(output_path, encoding="utf-8-sig")
