from bs4 import BeautifulSoup
import pandas as pd
import os
import ssl
import urllib.request
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from bs4 import BeautifulSoup
import pandas as pd

def GetAllShareholderDistribution():
    url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"

    context = ssl._create_unverified_context()

    # 使用這個 context 來打開連線
    with urllib.request.urlopen(url, context=context) as response:
        df = pd.read_csv(response)

    # 列轉成欄位
    # 參考 https://stackoverflow.com/questions/63413708/transforming-pandas-dataframe-convert-some-row-values-to-columns
    df["key2"] = df.groupby("證券代號").cumcount() + 1
    s = (
        df.set_index(["資料日期", "證券代號", "key2"])
        .unstack()
        .sort_index(level=1, axis=1)
    )
    s.columns = s.columns.map("{0[0]}_{0[1]}".format)
    s = s.rename_axis([None], axis=1).reset_index()
    # s.to_csv('股東分布資料.csv',encoding='utf_8_sig')
    # print(s)

    retailHeaders = [
        "1-999",
        "1,000-5,000",
        "5,001-10,000",
        "10,001-15,000",
        "15,001-20,000",
        "20,001-30,000",
        "30,001-40,000",
        "40,001-50,000",
        "50,001-100,000",
    ]
    distributionRangeHeaders = retailHeaders + [
        "100,001-200,000",
        "200,001-400,000",
        "400,001-600,000",
        "600,001-800,000",
        "800,001-1,000,000",
        "1,000,001",
        "差異數調整",
        "合計",
    ]

    newTitle = ["資料日期", "證券代號"] + [
        distribution + title
        for distribution in distributionRangeHeaders
        for title in ["人數", "比例", "持股分級", "股數"]
    ]
    # print(newTitle)
    s.columns = newTitle

    # print(s)
    s["100張以下比例"] = s[
        [retailHeader + title for retailHeader in retailHeaders for title in ["比例"]]
    ].sum(axis=1)
    s["100張以下人數"] = s[
        [retailHeader + title for retailHeader in retailHeaders for title in ["人數"]]
    ].sum(axis=1)
    s = s.rename(
        columns={
            "100,001-200,000比例": "101-200張比例",
            "100,001-200,000人數": "101-200張人數",
            "200,001-400,000比例": "201-400張比例",
            "200,001-400,000人數": "201-400張人數",
            "400,001-600,000比例": "401-600張比例",
            "400,001-600,000人數": "401-600張人數",
            "600,001-800,000比例": "601-800張比例",
            "600,001-800,000人數": "601-800張人數",
            "800,001-1,000,000比例": "801-1000張比例",
            "800,001-1,000,000人數": "801-1000張人數",
            "1,000,001比例": "1000張以上比例",
            "1,000,001人數": "1000張以上人數",
        }
    )
    s["401-800張人數"] = s[["401-600張人數", "601-800張人數"]].sum(axis=1)
    s["401-800張比例"] = s[["401-600張比例", "601-800張比例"]].sum(axis=1)
    # print(s.columns)
    s = s[
        [
            "證券代號",
            "100張以下人數",
            "100張以下比例",
            "101-200張人數",
            "101-200張比例",
            "201-400張人數",
            "201-400張比例",
            "401-800張人數",
            "401-800張比例",
            "801-1000張人數",
            "801-1000張比例",
            "1000張以上人數",
            "1000張以上比例",
        ]
    ]

    print(s)
    # s.to_csv(f'{GetRootPath()}\Data\Weekly\股東分布資料.csv',encoding='utf_8_sig')
    return s

def GetShareholderDistribution(stockId):
    url = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"

    # 創建一個新的 Chrome 瀏覽器實例
    webdriver_service = Service(ChromeDriverManager().install())
    
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')  # 啟動隱藏模式
    chrome_options.add_argument('--disable-gpu')  # windowsd必須加入此行 原文網址：https://itw01.com/FYB2UED.html
    browser = webdriver.Chrome(service=webdriver_service, options=chrome_options)

    # 讓瀏覽器打開指定的網址
    browser.get(url)

    # 獲取網頁的 HTML 內容
    html = browser.page_source

    # 使用 BeautifulSoup 解析 HTML
    soup = BeautifulSoup(html, "html.parser")

    select = soup.find("select", {"id": "scaDate"})
    options = [option.text for option in select.find_all("option")]
    top5_dates = options[:5]
    
    accumulator = {
        "100張以下比例": [],
        "100張以下人數": [],
        "100-1000張比例": [],
        "100-1000張人數": [],
        "1000張以上比例": [],
        "1000張以上人數": []
    }

    for lastDate in top5_dates:
        print(lastDate)

        # 輸入日期
        select = Select(browser.find_element(By.ID, "scaDate"))
        select.select_by_visible_text(lastDate)

        # 輸入股票代碼
        browser.find_element(By.NAME, "stockNo").send_keys(stockId)

        # 送出查詢
        browser.find_element(By.XPATH, "//*[@id=\"form1\"]/table/tbody/tr[4]/td/input").click()

        # 取得網頁原始碼
        html_file = browser.page_source

        # 傳入html file
        # 建立beautifulSoup 解析文件
        soup = BeautifulSoup(html_file, "lxml")

        # 找出回傳之分散表
        table = soup.find("table", class_="table")
        if table is not None:
            # 將表格轉換為 DataFrame
            df = pd.read_html(str(table))[0]
            print(df)
        else:
            print("No table with class 'table' found.")

        # 將 '持股/單位數分級' 列的值轉換為整數，以便於分組
        df['持股/單位數分級'] = df['持股/單位數分級'].str.replace(',', '').str.extract(r'(\d+)').astype(float)
        
        # '人數'
        df['人數'] = df['人數'].astype(float)

        # 定義分組的邊界
        bins = [0, 100, 1000, float('inf')]

        # 定義每組的標籤
        labels = ['100張以下比例', '100-1000張比例', '1000張以上比例']

        # 使用 pd.cut 函數將 '持股/單位數分級' 列的值分組(1張 1000股)
        df['Group'] = pd.cut(df['持股/單位數分級'], bins=[item * 1000 for item in bins], labels=labels)
        
        # 對每組的 '占集保庫存數比例 (%)' 和 '人數' 列的值進行加總
        group_result = df.groupby('Group').agg({'占集保庫存數比例 (%)': 'sum', '人數': 'sum'}).reset_index()
        group_result['人數'] = group_result['人數'].astype(int)
        
        print(group_result)
        shareholder_distribution = {
            "100張以下比例" : group_result.loc[0, "占集保庫存數比例 (%)"],
            "100張以下人數" :  group_result.loc[0, "人數"],
            "100-1000張比例": group_result.loc[1, "占集保庫存數比例 (%)"],
            "100-1000張人數" : group_result.loc[1, "人數"],
            "1000張以上比例": group_result.loc[2, "占集保庫存數比例 (%)"],
            "1000張以上人數": group_result.loc[2, "人數"]
        }
        
            
        for key in accumulator:
            accumulator[key].append(shareholder_distribution[key])
            
        # After processing all dates, format the accumulated values
        for key, values in accumulator.items():
            # Convert each value to string and format with 2 decimal places
            formatted_values = ["{:.2f}".format(value) for value in values]
            # Join the formatted values with " / "
            accumulator[key] = " / ".join(formatted_values)
        
    return pd.DataFrame([shareholder_distribution]) 
# ------ 測試 ------
# 總表
# WriteData()

# 個股(含歷程)
df = GetShareholderDistribution(2330)
print(df)

# print(GetAllShareholderDistribution())
