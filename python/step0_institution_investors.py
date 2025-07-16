import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from utils import (
    sleep,
    get_business_day,
    init,
    fetch_data,
    format_number,
    format_date_to_chinese,
    convert_to_billion
)


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
    response = fetch_data(url)
    return response.json()


def process_investors_data(json_data, amount_df, date_str):
    """處理法人資料"""
    df = pd.DataFrame(json_data["data"], columns=json_data["fields"])

    # 尋找合計行索引（避免硬編碼）
    idx = df[df["單位名稱"] == "合計"].index[0]

    # 計算法人總成交金額與市場比重
    buy_amt = pd.to_numeric(df.loc[idx, "買進金額"].replace(",", ""))
    sell_amt = pd.to_numeric(df.loc[idx, "賣出金額"].replace(",", ""))
    institutional_total = (buy_amt + sell_amt) / 2

    # 取得市場總額並計算比重
    market_total = amount_df.loc["總成交金額", date_str]
    inst_ratio = (institutional_total / market_total * 100).round(2)

    # 處理三個欄位並轉為億元單位
    columns_to_convert = ["買進金額", "賣出金額", "買賣差額"]
    df[columns_to_convert] = df[columns_to_convert].applymap(lambda x: convert_to_billion(x))

    # 替換外資及陸資名稱，移除「(不含外資自營商)」部分
    df.loc[df["單位名稱"] == "外資及陸資(不含外資自營商)", "單位名稱"] = "外資及陸資"

    # 一次性處理所有資料轉換，保留三個欄位
    result = df[["單位名稱", "買進金額", "賣出金額", "買賣差額"]].rename(columns={"單位名稱": "項目", })

    # 建立標題列
    title_row = pd.DataFrame([{
        "項目": f"{format_date_to_chinese(date_str)} 三大法人買賣超統計",
        f"買進金額": "",
        f"賣出金額": "",
        f"買賣差額": ""
    }])

    # 將標題列與結果合併
    result = pd.concat([title_row, result], ignore_index=True)

    # 新增市場總額與法人比重
    market_info = pd.DataFrame([
        {
            "交易總額": f"{(convert_to_billion(market_total)):.2f}",
            "法人": f"{(convert_to_billion(institutional_total)):.2f} ({inst_ratio}%)",
        }
    ])
    return result, market_info


def get_institutional_investors_exchange(day_count=1):
    """Fetch institutional investors exchange data for a given number of days."""
    amount_df = get_daily_exchange_amount(day_count)
    sum_df = pd.DataFrame()
    market_info_df = None
    count = 0

    while sum_df.shape[1] < day_count:
        temp_date = datetime.today() - pd.tseries.offsets.BDay(count)
        mingo_date_str = str(temp_date.year - 1911) + "/" + temp_date.strftime("%m/%d")
        json_data = fetch_investors_data(temp_date)

        if json_data["stat"] == "OK":
            df, market_info = process_investors_data(json_data, amount_df, mingo_date_str)
            sum_df = pd.merge(sum_df, df, on=["項目"]) if not sum_df.empty else df
            market_info_df = market_info

        count += 1
        if day_count > 1:
            sleep()  # 增加延遲時間

    # sum_df = sum_df.set_index("項目")
    return sum_df, market_info_df


def send_line_notify(df, market_info):
    """使用 LINE Message API 發送訊息"""
    from linebot import LineBotApi
    from linebot.models import TextSendMessage
    from linebot.exceptions import LineBotApiError
    from dotenv import load_dotenv
    import os

    # 載入環境變數
    load_dotenv()

    # 從環境變數讀取 token
    channel_access_token = os.getenv('ChannelAccessToken')
    user_id = os.getenv('UserId')
    line_bot_api = LineBotApi(channel_access_token)

    flex_message = create_flex_message(df, market_info)

    try:
        # 發送文字訊息
        line_bot_api.push_message(user_id, flex_message)
        return 200
    except LineBotApiError as e:
        print(f"LINE 訊息發送失敗: {e}")
        return e.status_code


def create_flex_message(df, market_info):
    from linebot.models import FlexSendMessage, BubbleContainer, BoxComponent

    # 建立 Flex Message 的內容
    contents = []

    # 添加標題
    contents.append({
        "type": "text",
        "text": df.iloc[0]["項目"],
        "weight": "bold",
        "size": "xl"
    })

    # 添加分隔線
    contents.append({"type": "separator"})

    # 添加欄位標題
    contents.append({
        "type": "box",
        "layout": "baseline",
        "contents": [
            {
                "type": "text",
                "text": col,
                "weight": "bold",
                "size": "sm" if i > 0 else "md",
                "align": "end" if i > 0 else "start",
                "flex": 3 if i > 0 else 6
            }
            for i, col in enumerate(df.columns)
        ],
        "margin": "md"
    })

    # 添加另一條分隔線
    contents.append({"type": "separator", "margin": "sm"})

    # 獲取欄位名稱
    buy_col = [col for col in df.columns if '買進金額' in col][0]
    sell_col = [col for col in df.columns if '賣出金額' in col][0]
    diff_col = [col for col in df.columns if '買賣差額' in col][0]

    # 跳過第一列（標題列）
    data_df = df.iloc[1:].copy()  # 第一列是標題，從第二列開始是真實資料

    # 設定 "項目" 為索引，僅用於資料處理
    data_df = data_df.set_index("項目")

    # 迭代 DataFrame 的每一行，添加資料
    for idx, row in data_df.iterrows():
        item_name = idx  # 直接使用索引作為項目名稱

        if item_name == "合計":
            # 添加另一條分隔線
            contents.append({"type": "separator", "margin": "sm"})

        # 處理買進金額
        try:
            buy_value = float(row[buy_col])
            formatted_buy = format_number(buy_value)
        except:
            formatted_buy = "N/A"

        # 處理賣出金額
        try:
            sell_value = float(row[sell_col])
            formatted_sell = format_number(sell_value)
        except:
            formatted_sell = "N/A"

        # 處理買賣差額
        try:
            diff_value = float(row[diff_col])
            diff_color = "#FF0000" if diff_value > 0 else "#28a745"
            formatted_diff = format_number(diff_value)
        except:
            diff_color = "#000000"
            formatted_diff = "N/A"

        contents.append({
            "type": "box",
            "layout": "baseline",
            "contents": [
                {
                    "type": "text",
                    "text": f"{item_name}",
                    "size": "md",
                    "flex": 6
                },
                {
                    "type": "text",
                    "text": formatted_buy.replace(" ", "\u200B"),
                    "size": "sm",
                    "align": "end",
                    "flex": 3
                },
                {
                    "type": "text",
                    "text": formatted_sell.replace(" ", "\u200B"),
                    "size": "sm",
                    "align": "end",
                    "flex": 3
                },
                {
                    "type": "text",
                    "text": formatted_diff.replace(" ", "\u200B"),
                    "size": "sm",
                    "color": diff_color,
                    "align": "end",
                    "flex": 3
                }
            ],
            "spacing": "md",
            "margin": "md"
        })

    # 添加粗分隔線
    contents.append({
        "type": "separator",
        "margin": "xl",
        "color": "#0000FF"
    })

    # 添加市場資訊標題
    contents.append({
        "type": "text",
        "text": "市場資訊",
        "weight": "bold",
        "size": "lg",
        "margin": "md"
    })

    # 市場資訊資料 - 使用 for 迴圈直接處理所有欄位
    if not market_info.empty:
        row = market_info.iloc[0]  # 取得第一列資料

        # 使用 for 迴圈遍歷所有欄位
        column_contents = []

        for col_name in market_info.columns:

            # 添加欄位名稱和對應值的文字物件
            column_contents.extend([
                {
                    "type": "text",
                    "text": f"{col_name} : {row[col_name]}",
                    "size": "md",
                    "flex": 6
                }
            ])

        # 建立包含所有欄位的單一行
        contents.append({
            "type": "box",
            "layout": "baseline",
            "contents": column_contents,
            "margin": "md"
        })

    # 建構 Bubble Container
    bubble = BubbleContainer(
        body=BoxComponent(
            layout="vertical",
            contents=contents
        ),
        size="giga"  # 設定整體大小，可選 nano、micro、kilo、mega、giga
    )

    # 回傳 Flex Message
    return FlexSendMessage(alt_text="三大法人買賣超統計", contents=bubble)


def main():
    """主程式"""
    init()
    df, market_info = get_institutional_investors_exchange(1)

    if not df.empty:
        print("\n三大法人:")
        print(df)
        print("\n市場資訊:")
        print(market_info)
        Path("public").mkdir(exist_ok=True)

        # 建立空白分隔列
        empty_row = pd.DataFrame([{col: "" for col in df.columns}])

        # 格式化市場資訊
        market_info_formatted = pd.DataFrame([
            {
                "項目": "市場交易資訊",
                df.columns[1]: f"{market_info.iloc[0]['交易總額']} 億元",
                df.columns[2]: f"{market_info.iloc[0]['法人']}",
                df.columns[3]: ""
            }
        ])

        # 合併所有資料
        combined_df = pd.concat([
            df,                     # 三大法人資料
            empty_row,              # 空白分隔列
            market_info_formatted   # 市場資訊
        ], ignore_index=True)

        combined_df.to_csv("public/institutional_investors_exchange.csv", index=False, encoding="utf-8-sig")

        print("資料已儲存至 public/institutional_investors_exchange.csv")
    else:
        print("無法獲取資料")

    # 發送 LINE 通知
    response_code = send_line_notify(df, market_info)
    if response_code == 200:
        print("LINE 通知已成功發送")
    else:
        print(f"LINE 通知發送失敗，錯誤碼: {response_code}")


if __name__ == "__main__":
    main()
