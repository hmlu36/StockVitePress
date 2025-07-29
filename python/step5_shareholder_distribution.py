from bs4 import BeautifulSoup
import pandas as pd
import ssl
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
from playwright.sync_api import sync_playwright


def get_all_shareholder_distribution():
    url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"

    context = ssl._create_unverified_context()

    # 使用這個 context 來打開連線
    with urllib.request.urlopen(url, context=context) as response:
        df = pd.read_csv(response)

    df["證券代號"] = df["證券代號"].str.strip()
    # 篩選四碼數字的證券代號
    df = df[df["證券代號"].str.match(r'^\d{4}$')]

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

    # s.to_csv(f'{GetRootPath()}\Data\Weekly\股東分布資料.csv',encoding='utf_8_sig')
    return s


def get_shareholder_distribution(stockId):
    with sync_playwright() as p:
        # 建立瀏覽器實例
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            url = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
            page.goto(url)

            # 等待選單元素載入
            page.wait_for_selector("#scaDate")

            # 獲取日期選項
            options = page.eval_on_selector_all("#scaDate option", """
                (options) => options.map(option => option.text)
            """)
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

                # 選擇日期
                page.select_option("#scaDate", label=lastDate)

                # 輸入股票代碼
                page.fill('input[name="stockNo"]', str(stockId))

                # 點擊查詢按鈕
                page.click('input[value="查詢"]')

                # 等待表格出現
                page.wait_for_selector(".table")

                # 獲取表格HTML
                table_html = page.eval_on_selector(".table", "table => table.outerHTML")

                # 解析表格
                soup = BeautifulSoup(table_html, "lxml")
                df = pd.read_html(str(soup))[0]
                print(df)

                # 將 '持股/單位數分級' 列的值轉換為整數
                df['持股/單位數分級'] = df['持股/單位數分級'].str.replace(',', '').str.extract(r'(\d+)').astype(float)

                # '人數'
                df['人數'] = df['人數'].astype(float)

                # 定義分組的邊界
                bins = [0, 100, 1000, float('inf')]

                # 定義每組的標籤
                labels = ['100張以下比例', '100-1000張比例', '1000張以上比例']

                # 使用 pd.cut 函式將 '持股/單位數分級' 列的值分組(1張 1000股)
                df['Group'] = pd.cut(df['持股/單位數分級'], bins=[item * 1000 for item in bins], labels=labels)

                # 對每組的 '占集保庫存數比例 (%)' 和 '人數' 列的值進行加總
                group_result = df.groupby('Group').agg({'占集保庫存數比例 (%)': 'sum', '人數': 'sum'}).reset_index()
                group_result['人數'] = group_result['人數'].astype(int)

                print(group_result)
                shareholder_distribution = {
                    "100張以下比例": group_result.loc[0, "占集保庫存數比例 (%)"],
                    "100張以下人數": group_result.loc[0, "人數"],
                    "100-1000張比例": group_result.loc[1, "占集保庫存數比例 (%)"],
                    "100-1000張人數": group_result.loc[1, "人數"],
                    "1000張以上比例": group_result.loc[2, "占集保庫存數比例 (%)"],
                    "1000張以上人數": group_result.loc[2, "人數"]
                }

                for key in accumulator:
                    accumulator[key].append(shareholder_distribution[key])

            # 處理所有日期後，格式化累積的值
            for key, values in accumulator.items():
                # 將每個值轉換為字串並格式化為2位小數
                formatted_values = ["{:.2f}".format(value) for value in values]
                # 使用 " / " 連接格式化的值
                accumulator[key] = " / ".join(formatted_values)

            return pd.DataFrame([shareholder_distribution])

        finally:
            browser.close()
# ------ 測試 ------
# 總表
# WriteData()


# 個股(含歷程)
#df = get_shareholder_distribution(2330)

# df = GetAllShareholderDistribution()
#print(df)
