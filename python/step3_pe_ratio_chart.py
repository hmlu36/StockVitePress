import pandas as pd
import re
import random
import time
import utils
"""
抓取本益比
取得現今EPS、本益比、近五年六個級距本益比

選股條件：
1. 本益比小於10
2. 小於近五年最小級距本益比
"""


def get_pe(stockId):
    url = f"https://goodinfo.tw/tw/ShowK_ChartFlow.asp?RPT_CAT=PER&STOCK_ID={stockId}&CHT_CAT=WEEK"
    css_selector = "#divDetail"
    try:
        list = utils.get_dataframe_by_css_selector(url, css_selector, 2)
        #print(list)
        # 取前兩列後面倒數6欄資料, 轉成DataFrame
        firstRowDf = list.iloc[:1, -6:]
        #print(firstRowDf)
    except:
        time.sleep(random.randint(20, 30))
        df = utils.get_dataframe_by_css_selector(url, css_selector, 2)

        # 取前兩列後面倒數6欄資料
        firstRowDf = list.iloc[:1, -6:]
        #print(firtRowDf)
    
    #print(firstRowDf)
    
    # dataframe轉成dictionary 參考 https://stackoverflow.com/questions/45452935/pandas-how-to-get-series-to-dict
    dictionaries = [
        dict(
            key=float(re.findall(r'[0-9]+[.]?[0-9]*', str(k))[0]), 
            value=v.str.extract(r"([-+]?\d*\.\d+|\d+)")[0].astype(float).iloc[0]
        ) 
        for k, v in firstRowDf.items()
    ]
   
    #print(dictionaries)

    # 轉換成dataframe
    data = []
    headers = [
        "本益比-級距1倍數",
        "本益比-級距1價格",
        "本益比-級距2倍數",
        "本益比-級距2價格",
        "本益比-級距3倍數",
        "本益比-級距3價格",
        "本益比-級距4倍數",
        "本益比-級距4價格",
        "本益比-級距5倍數",
        "本益比-級距5價格",
        "本益比-級距6倍數",
        "本益比-級距6價格",
    ]
    for entry in dictionaries:
        #print(entry)
        data.append(entry["key"])
        data.append(entry["value"])

    #print(headers)
    #print(data)
    df = pd.DataFrame([data], columns=headers)
    return df

# ------ 測試 ------

#data = get_pe('2330')
#print(data)

