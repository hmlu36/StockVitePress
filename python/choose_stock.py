import pandas as pd
from datetime import datetime, timedelta, date
import time
import random
from step1_basic_stock_info import get_basic_stock_info
from step2_fin_detail import get_fin_detail
from step3_pe_ratio_chart import get_pe
from step4_k_chart import get_transaction
import step5_shareholder_distribution as shareholderDistribution
from step6_stock_dividend_policy import get_dividend
from step7_volume_data import get_volume
import step8_director_shareholder as get_director_shareholder
import step9_daily_top_Volume as daily_top_volume
import csv
import utils
import pathlib

"""
選股條件：
（1）評估價值是否被低估？（股票價格不會太貴）
1. 本益比　　　< 15倍
2. 現金殖利率　> 5 %

（2）本益比低估
1. 本益比小於10
2. 小於近五年最小級距本益比

（3）確定本業利益是成長的，且為本業賺的（不是靠業外收益賺的，獲利不持久）
1. 營收累計年增率 > 0 %
2. 毛利率 > 0 %
3. 營業利益率 > 0 %
4. 稅前淨利率 > 0 %
5. 稅後淨利率 > 0 %
6. 本業收益（營業利益率／稅前淨利率） > 60 %
7. ROE > 10
"""

stocks = [
    # '1229', '1231', '1409', '1304', '1308', '1474', '1515', '1604', '2020',
    # '2069', '2324', '2347',
    # '2352', '2385', '2387', '2417', '2458', '2488',
    # '2520', '2546', '2881', '3005', '3028', '3033',
    # '3044', '3048',
    # '3209', '3231',
    # '3312', '3702',
    # '3706', '6257', '8112', '8150'
    '2330'
]


def Sleep():
    time.sleep(random.randint(10, 20))


def GetChampionStock(op):
    # 過濾清單
    if op == 0:
        df = get_basic_stock_info(True)
        print(df)

        df.update(df.apply(lambda x: pd.to_numeric(x, errors='coerce')))

        cond1 = df['毛利率'] > 30
        cond2 = df['營業利益率'] > 30
        cond3 = df['本益比'] < 15
        cond3 = df['資本額'] > 15
        df = df[cond1 & cond2 & cond3]
        print(df)

        df.to_csv(f"{utils.GetRootPath()}\\Data\\Temp\\過濾清單.csv", encoding="utf_8_sig")

    # 明細資料
    if op == 2:
        basicStockInfo_df = get_basic_stock_info()
        # sum_df = pd.DataFrame()

        for stockId in stocks:
            print(stockId)

            stockInfo_df = basicStockInfo_df[basicStockInfo_df["證券代號"] == stockId]
            stockInfo_df.reset_index(drop=True, inplace=True)
            print(stockInfo_df)

            if not stockInfo_df.empty:
                Sleep()
                finDetail_df = get_fin_detail(stockId)
                print(finDetail_df)

                PE_df = get_pe(stockId)
                print(PE_df)

                Sleep()
                transaction_df = get_transaction(stockId)
                print(transaction_df)

                volume_df = get_volume(stockId)
                print(volume_df)

                Sleep()
                dividend_df = get_dividend(stockId)
                print(dividend_df)

                Sleep()
                distribution_df = shareholderDistribution.GetShareholderDistribution(stockId)
                print(distribution_df)

                # 合併所有欄位成一列
                temp_df = pd.concat([stockInfo_df, transaction_df, volume_df, PE_df, distribution_df, finDetail_df, dividend_df], axis=1)
                print(temp_df)

                # 將列合併入dataframe
                # sum_df = pd.concat([sum_df, temp_df], axis=0)

                # 每列寫入csv檔, 不含表頭
                temp_df.to_csv(f"{utils.GetRootPath()}\\Data\\Temp\\彙整清單.csv", mode="a", header=False, index=False, encoding="utf_8_sig")

        # 寫入csv檔
        # sum_df.to_csv('彙整清單.csv', encoding='utf_8_sig')

    # 日常籌碼面資料
    if op == 3:
        basicStockInfo_df = get_basic_stock_info()
        # sum_df = pd.DataFrame()
        for stockId in stocks:
            print(stockId)

            stockInfo_df = basicStockInfo_df[basicStockInfo_df["證券代號"] == stockId]
            stockInfo_df.reset_index(drop=True, inplace=True)
            print(stockInfo_df)

            if not stockInfo_df.empty:
                Sleep()
                transaction_df = get_transaction(stockId)
                print(transaction_df)

                volume_df = get_volume(stockId)
                print(volume_df)

                temp_df = pd.concat([stockInfo_df, transaction_df, volume_df], axis=1)
                print(temp_df)

                temp_df.to_csv(f"{utils.GetRootPath()}\\Data\\Daily\\籌碼面資料.csv", mode="a", header=False, encoding="utf_8_sig")
                # 合併所有欄位成一列
                # sum_df = pd.concat([sum_df, temp_df], axis=0)

        # 將列合併入dataframe
        # sum_df.to_csv('籌碼面資料.csv',encoding='utf_8_sig')

    # 大戶、本益比
    if op == 4:
        shareholderDistribution.WriteData()

        for stockId in stocks:
            print(stockId)

            Sleep()
            distribution_df = shareholderDistribution.GetDistribution(stockId)
            print(distribution_df)

            Sleep()
            PE_df = get_pe(stockId)
            print(PE_df)

            temp_df = pd.concat([PE_df, distribution_df], axis=1)
            print(temp_df)

            temp_df.to_csv(f'{utils.GetRootPath()}\\Data\\\\Weekly\\股東分布_本益比_{date.today().strftime("%Y%m%d")}.csv', mode="a", header=False, encoding="utf_8_sig")

    if op == 5:
        get_director_shareholder.WriteData()

    if op == 7:
        basicStockInfo_df = get_basic_stock_info()
        topVolumeStocks = daily_top_volume.get_top_volume()[:100]

        for stockId in topVolumeStocks:
            print(stockId)

            stockInfo_df = basicStockInfo_df[basicStockInfo_df["證券代號"] == stockId]
            stockInfo_df.reset_index(drop=True, inplace=True)
            print(stockInfo_df)

            if not stockInfo_df.empty:
                volume_df = get_volume(stockId)
                print(volume_df)

                temp_df = pd.concat([stockInfo_df, volume_df], axis=1)
                print(temp_df)

                temp_df.to_csv(f'{utils.GetRootPath()}\\Data\\Daily\\異常籌碼資料_{date.today().strftime("%Y%m%d")}.csv', mode="a", header=False, encoding="utf_8_sig")

        # 刪除暫存檔案
        try:
            folderPath = pathlib.Path(f'{utils.GetRootPath()}\\Data\\Daily\\Chip\\{(date.today() - timedelta(days=1)).strftime("%Y%m%d")}')
            utils.delete_folder(folderPath)
        except Exception as ex:
            print(ex)


# 0 產生過濾清單(本益比、殖利率、淨值比、收盤價、全體董監持股、股東分布人數)
# 1 產生過濾清單(同0含本益比)
# 2 抓出股票明細資料
# 3 日排程 - 籌碼面資料
# 4 週排程 - 大戶、本益比
# 5 月排程 - 董監比例
# 6 季排程 - 財務資料
# 7 日排程 - 異常買入
GetChampionStock(2)
