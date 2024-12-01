from io import StringIO
import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
from datetime import datetime
import os
import src.Utils2 as Utils2
import pyuser_agent

def GetDividend(stockId):
    url = f'https://goodinfo.tw/tw/StockDividendPolicy.asp?STOCK_ID={stockId}'
    cssSelector = '#divDetail'
    try:
        df = Utils2.GetDataFrameByCssSelector(url, cssSelector)
        df.columns = df.columns.get_level_values(3)
    except:
        time.sleep(random.randint(20, 30))
        df = Utils2.GetDataFrameByCssSelector(url, cssSelector)
        df.columns = df.columns.get_level_values(3)

    # column replace space
    df.columns = df.columns.str.replace(' ', '')

    # filter not  ∟
    df = df[df['股利發放年度'] != '∟']
    #print(df)

    # 年度大於2022, 移除第一列
    firstRow = df.iloc[0, :]
    if int(firstRow['股利發放年度']) > datetime.now().year:
        df = df.iloc[1: , :]

    rowsCount = 5
    # 年度(取前5筆, index重新排序)
    year = pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna(how='any',axis=0).head(rowsCount).astype(int).reset_index(drop=True)
    #print(year)

    # 現金(取前5筆, index重新排序)
    cash = pd.to_numeric(df.iloc[:, 3], errors='coerce').dropna(how='any',axis=0).head(rowsCount).reset_index(drop=True)
    #print(cash)
    
    # 股票(取前5筆, index重新排序)
    stock = pd.to_numeric(df.iloc[:, 6], errors='coerce').dropna(how='any',axis=0).head(rowsCount).reset_index(drop=True)
    #print(stock)

    data = []
    for index in range(0, rowsCount):
        data.append(str(cash[index]).rjust(6) + ' / ' + str(stock[index]).rjust(6))

    print(data)
    df = pd.DataFrame([data], columns=year)
    
    return df


def GetAllDividend():    
    cssSelector = '#divStockList'
    
    for rankIndex in range(0, 6):
        
        url = f'https://goodinfo.tw/tw/StockList.asp?SHEET=股利政策&MARKET_CAT=熱門排行&INDUSTRY_CAT=合計股利&RANK={str(rankIndex)}'
        print(url)
        
        # 休息10~20秒
        time.sleep(random.randint(10, 20))

        try:
            df = GetDataFrameByCssSelector(url, cssSelector)
            #return df
        except:
            time.sleep(random.randint(20, 30))
            df = GetDataFrameByCssSelector(url, cssSelector)
            print(df)
            #df.columns = df.columns.get_level_values(1)

        df.columns = df.columns.get_level_values(0)
        df = df.drop_duplicates(keep=False, inplace=False) #移除重複標題
        #gain = pd.to_numeric(df['漲跌  價'], errors='coerce') > 0
        #market = df['市  場'] == '市'
        print(df)
        length = df['代號'].astype(str).map(len) == 4
        #df = df[gain & length]
        df = df[length]

        filePath = f'{Utils2.GetRootPath()}\Data\Yearly\合計股利.csv'
        if rankIndex == 0:
            df.to_csv(filePath, encoding='utf_8_sig')
        else:
            df.to_csv(filePath, mode='a', header=False, encoding='utf_8_sig')
        # 去除重複標頭
        #sum_df[sum_df.ne(sum_df.columns).any(1)].to_csv(f'{GetRootPath()}\Data\Monthly\董監持股比例.csv',encoding='utf_8_sig')

    print('執行完成')

'''
    data = pd.to_numeric(df['＞1千張'], errors='coerce').dropna(how='any',axis=0).head(3)
    return ' / '.join(map(str, list(data)))
'''



# ------ 測試 ------

#df = GetDividend('2356')
#print(df)
