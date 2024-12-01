from io import StringIO
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
import os
import pyuser_agent
import requests

def GetNetWorth():    
    cssSelector = '#divStockList'

    sum_df = pd.DataFrame()

    for rankIndex in range(0, 5):
        url = f'https://goodinfo.tw/tw2/StockList.asp?MARKET_CAT=熱門排行&INDUSTRY_CAT=每股淨值最高@@每股淨值@@每股淨值最高&SHEET=季資產狀況&SHEET2=資產負債金額&RANK={str(rankIndex)}'
        print(url)
        try:
            time.sleep(random.randint(5, 10))
            df = GetDataFrameByCssSelector(url, cssSelector)
            print(df)
            sum_df = pd.concat([sum_df, df], axis=0)
            # df.columns = df.columns.get_level_values(1)
        except:
            time.sleep(random.randint(20, 30))
            df = GetDataFrameByCssSelector(url, cssSelector)
            print(df)
            # df.columns = df.columns.get_level_values(1)

    # 去除重複標頭
    sum_df = sum_df[sum_df.ne(sum_df.columns).any(1)]
    return sum_df[["代號", "每股  淨值  (元)"]].rename(columns={"代號": "證券代號", "每股  淨值  (元)": "淨值"})

# ------ 共用的 function ------
def GetDataFrameByCssSelector(url, css_selector):
    ua = pyuser_agent.UA()
    user_agent = ua.random
    headers = {"user-agent": user_agent}
    rawData = requests.get(url, headers=headers)
    rawData.encoding = "utf-8"
    soup = BeautifulSoup(rawData.text, "html.parser")
    data = soup.select_one(css_selector)
    try:
        dfs = pd.read_html(StringIO(data.prettify()))
    except:
        return pd.DataFrame()

    #print(dfs)
    if len(dfs[0]) > 1:
        return dfs[0]
    if len(dfs[1]) > 1:
        return dfs[1]
    return dfs
    
def GetRootPath():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ------ 測試 ------

#df = GetNetWorth()
#print(df)
#df.to_csv(f'{GetRootPath()}\Data\Temp\export.csv',encoding='utf_8_sig')