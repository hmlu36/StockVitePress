from io import StringIO
from bs4 import BeautifulSoup
import requests
import random
import time
import pandas as pd
import os

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import src.Utils2 as Utils2


def GetStockBoardTop():
    # 取自神秘金字塔
    url = "https://norway.twsthr.info/StockBoardTop.aspx"
    cssSelector = "#details"
    df = Utils2.GetDataFrameByCssSelector(url, cssSelector)
    df.columns = df.columns.get_level_values(0)
    df = df.iloc[:, [3, 7]]
    
    df['證券代號'] = df['個股代號/名稱'].str[0:4]
    df['公司名稱'] = df['個股代號/名稱'].str[4:]
    df = df[['證券代號', '公司名稱', '持股比率 %']]
    df = df.rename(columns={"持股比率 %": "全體董監持股(%)"})
    #df.to_csv(f"{GetRootPath()}\Data\Monthly\董監持股比例.csv", encoding="utf_8_sig")
    return df


def GetDirectorSharehold():
    cssSelector = "#divStockList"
    sum_df = pd.DataFrame()

    for rankIndex in range(0, 6):
        url = f"https://goodinfo.tw/tw/StockList.asp?SHEET=董監持股&MARKET_CAT=熱門排行&INDUSTRY_CAT=全體董監持股比例&RANK={str(rankIndex)}"
        print(url)

        try:
            time.sleep(random.randint(5, 10))
            df = Utils2.GetDataFrameByCssSelector(url, cssSelector)
            print(df)
            sum_df = pd.concat([sum_df, df], axis=0)
            # df.columns = df.columns.get_level_values(1)
        except:
            time.sleep(random.randint(20, 30))
            df = Utils2.GetDataFrameByCssSelector(url, cssSelector)
            print(df)
            # df.columns = df.columns.get_level_values(1)

    # 去除重複標頭
    sum_df = sum_df[~(sum_df == sum_df.columns).all(axis=1)]
    # sum_df.to_csv(f'{GetRootPath()}\Data\Monthly\董監持股比例.csv',encoding='utf_8_sig')
    return sum_df

# ------ 測試 ------
#print(GetDirectorSharehold())
#print(GetStockBoardTop())
