from io import StringIO
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
import os
import pyuser_agent
import requests

def GetTopVolume():    
    cssSelector = '#divStockList'

    url = f'https://goodinfo.tw/tw/StockList.asp?RPT_TIME=&MARKET_CAT=熱門排行&INDUSTRY_CAT=日成交張數創近期新高日數@@成交張數@@日成交張數創近期新高日數'
    print(url)

    try:
        df = GetDataFrameByCssSelector(url, cssSelector)
        #return df
    except:
        time.sleep(random.randint(20, 30))
        df = GetDataFrameByCssSelector(url, cssSelector)
        print(df)
        #df.columns = df.columns.get_level_values(1)

    df.columns = df.columns.get_level_values(0)
    df = df.drop_duplicates(keep=False, inplace=False)
    #gain = pd.to_numeric(df['漲跌  價'], errors='coerce') > 0
    #market = df['市  場'] == '市'
    length = df['代號'].astype(str).map(len) == 4
    #df = df[gain & length]
    df = df[length]
    df.to_csv(f'{GetRootPath()}\\Data\\Daily\\日成交張數創近期新高日數.csv', encoding='utf_8_sig')
    return df['代號'].values
    # 去除重複標頭
    #sum_df[sum_df.ne(sum_df.columns).any(1)].to_csv(f'{Utils.GetRootPath()}\Data\Monthly\董監持股比例.csv',encoding='utf_8_sig')


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

    # print(dfs)
    if len(dfs[0]) > 1:
        return dfs[0]
    if len(dfs[1]) > 1:
        return dfs[1]
    return dfs
    
def GetRootPath():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ------ 測試 ------

# df = GetTopVolume()
# print(df)
