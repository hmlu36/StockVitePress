import random
import time
import pandas as pd
import utils


def get_stock_board_top():
    # 取自神秘金字塔
    url = "https://norway.twsthr.info/StockBoardTop.aspx"
    cssSelector = "#details"
    df = utils.get_dataframe_by_css_selector(url, cssSelector)
    df.columns = df.columns.get_level_values(0)
    df = df.iloc[:, [3, 7]]
    
    df['證券代號'] = df['個股代號/名稱'].str[0:4]
    df['公司名稱'] = df['個股代號/名稱'].str[4:]
    df = df[['證券代號', '公司名稱', '持股比率 %']]
    df = df.rename(columns={"持股比率 %": "全體董監持股(%)"})
    #df.to_csv(f"{GetRootPath()}\Data\Monthly\董監持股比例.csv", encoding="utf_8_sig")
    return df


def get_director_shareholder():
    cssSelector = "#divStockList"
    sum_df = pd.DataFrame()

    for rankIndex in range(0, 6):
        url = f"https://goodinfo.tw/tw/StockList.asp?SHEET=董監持股&MARKET_CAT=熱門排行&INDUSTRY_CAT=全體董監持股比例&RANK={str(rankIndex)}"
        print(url)

        try:
            time.sleep(random.randint(5, 10))
            df = utils.get_dataframe_by_css_selector(url, cssSelector)
            print(df)
            sum_df = pd.concat([sum_df, df], axis=0)
            # df.columns = df.columns.get_level_values(1)
        except:
            time.sleep(random.randint(20, 30))
            df = utils.get_dataframe_by_css_selector(url, cssSelector)
            print(df)
            # df.columns = df.columns.get_level_values(1)

    # 去除重複標頭
    sum_df = sum_df[~(sum_df == sum_df.columns).all(axis=1)]
    # sum_df.to_csv(f'{GetRootPath()}\Data\Monthly\董監持股比例.csv',encoding='utf_8_sig')
    return sum_df

# ------ 測試 ------
print(get_director_shareholder())
print(get_stock_board_top())
