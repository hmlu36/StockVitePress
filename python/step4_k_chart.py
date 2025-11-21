import pandas as pd
import random
import time
import utils

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

def get_transaction(stockId):
    url = f'https://goodinfo.tw/tw/ShowK_Chart.asp?STOCK_ID={stockId}&CHT_CAT2=DATE'
    cssSelector = '#divDetail'
    try:
        df = utils.get_dataframe_by_css_selector(url, cssSelector)
        df.columns = df.columns.get_level_values(1)
    except:
        time.sleep(random.randint(20, 30))
        df = utils.get_dataframe_by_css_selector(url, cssSelector)
        df.columns = df.columns.get_level_values(1)
    # 印出全部的rows
    #pd.set_option('display.max_rows', df.shape[0]+1)
    #print(df)

    headers = ['收盤', '張數', '外資  持股  (%)', '券資  比  (%)']
    smaPeriods = [1, 5, 20, 60]

    dict = {}
    for header in headers:
        try:
            #print(header)
            entry = ''
            for period in smaPeriods:
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
                    

            dict.update({header.replace(' ', '') + '(' +  'ma / '.join(map(str, smaPeriods)) + 'ma)': str(entry)})
        except:
            dict.update({header.replace(' ', '') + '(' +  'ma / '.join(map(str, smaPeriods)) + 'ma)': ''})
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
#df = get_transaction('2330')
#print(df)