from io import StringIO
import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
import pyuser_agent
import src.Utils2 as Utils2

'''
url_root = 'https://goodinfo.tw/StockInfo/ShowK_Chart.asp'
payload = {
    'STOCK_ID': '8112',
    'CHT_CAT2': 'DATE',
    'STEP': 'DATA',
    'PERIOD': 365
}

cssSelector = '#divPriceDetail'
df = Utils.PostDataFrameByCssSelector(url_root, payload, cssSelector)
'''

def GetTransaction(stockId):
    url = f'https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={stockId}&CHT_CAT2=DATE'
    cssSelector = '#divPriceDetail'
    try:
        df = Utils2.GetDataFrameByCssSelector(url, cssSelector)
        df.columns = df.columns.get_level_values(1)
    except:
        time.sleep(random.randint(20, 30))
        df = Utils2.GetDataFrameByCssSelector(url, cssSelector)
        df.columns = df.columns.get_level_values(1)
    # 印出全部的rows
    #pd.set_option('display.max_rows', df.shape[0]+1)
    #print(df)

    headers = ['收盤', '張數', '外資  持股  (%)', '券資  比  (%)']
    smaPeroids = [1, 5, 20, 60]
    
    dict = {}
    for header in headers:
        try:
            #print(header)
            entry = ''
            for period in smaPeroids:
                #print(df[header])
                data = pd.to_numeric(df[header], errors='coerce').dropna(how='any',axis=0).head(period)
                #print(data)
                sma = round(data.mean(), 2)
                #print(sma)
                entry += ('' if entry == '' else ' / ') + str(sma).rjust(8)
            
            #print(header.replace(' ', ''))
            #print(entry)
            
            if header == '收盤':
                data = [x.strip() for x in entry.split('/')]
                prefixIcon = ''
                if float(data[0]) > float(data[1]) and float(data[0]) > float(data[2]):
                    prefixIcon = '👍' 
                elif float(data[0]) < float(data[3]):
                    prefixIcon = '👎'
                entry = prefixIcon + entry

            # 成交量 > 5ma 3倍
            if header == '張數':
                data = [x.strip() for x in entry.split('/')]
                if(float(data[0]) / float(data[1]) > 3.0):
                    entry = '🏆' + entry
                    

            dict.update({header.replace(' ', '') + '(' +  'ma / '.join(map(str, smaPeroids)) + 'ma)': str(entry)})
        except:
            dict.update({header.replace(' ', '') + '(' +  'ma / '.join(map(str, smaPeroids)) + 'ma)': ''})
    #print(dict)
    result = pd.DataFrame([dict])
    return result
        #print(row)
        #tempDf = pd.DataFrame({header.replace(' ', ''): row})
        #print(tempDf)
        #finalDf = pd.concat([finalDf, tempDf], axis=1)
    #print(finalDf)
    #return finalDf



# ------ 測試 ------
'''
df = GetTransaction('2330')
print(df)
'''