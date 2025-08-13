import utils
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd

"""
1. 營業收入累計年增率 > 0 %
2. 毛利率 > 0 %
3. 營業利益率 > 0 %
4. 稅前淨利率 > 0 %
5. 稅後淨利率 > 0 %
6. 本業收益（營業利益率／稅前淨利率） > 60 %
7. ROE > 10 %
8. 董監持股比例 > 20
"""
def get_fin_data(stockId, finType):
    # 損益表
    if finType == "income_statement":
        url = f"https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=IS_M_QUAR_ACC&STOCK_ID={stockId}"
    # 資產負債表
    elif finType == "balance_sheet":
        url = f"https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=BS_M_QUAR_ACC&STOCK_ID={stockId}"
    # 財務比率表
    elif finType == "financial_ratio":
        url = f"https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=XX_M_QUAR_ACC&STOCK_ID={stockId}"

    print(url)
    
    css_selector = "#txtFinBody"
    try:
        df = utils.get_dataframe_by_css_selector(url, css_selector)
    except:
        df = utils.get_dataframe_by_css_selector(url, css_selector)
    # print(df)

    # 所有columns和index名稱
    # print(df.columns)
    # print(df.iloc[:, 0])

    # 檢查 DataFrame 是否為空或沒有欄位
    if df.empty or df.shape[1] == 0:
        print(f"警告: 無法取得 {finType} 資料，DataFrame 為空")
        return pd.DataFrame()
    
    # 設置DataFrame的索引為第一列的值
    df.set_index(df.iloc[:, 0], inplace=True)
    return df


def get_fin_detail(stockId):
    df_is = get_fin_data(stockId, "income_statement")

    # 根據column名稱, 以及列的第一欄名稱取值
    # 取得年度季
    yearQuarter = df_is.columns[1][0]
    print(yearQuarter)

    # 營業收入 (含 其他收益及費損合計)
    operating_revenue = Decimal(df_is.loc["營業收入", (yearQuarter, "金額")]) + Decimal(
        df_is.loc["其他收益及費損合計", (yearQuarter, "金額")]
    )
    print(f"營業收入:{operating_revenue}")

    # 營業成本
    operating_costs = Decimal(df_is.loc["營業成本", (yearQuarter, "金額")])
    print(f"營業成本:{operating_costs}")

    # 營業費用
    operating_expenses = Decimal(df_is.loc["營業費用", (yearQuarter, "金額")])
    print(f"營業費用:{operating_expenses}")

    # 所得稅費用
    tax_expense = Decimal(df_is.loc["所得稅費用", (yearQuarter, "金額")])
    print(f"所得稅費用:{tax_expense}")

    # 業外損益合計
    non_operating_income_expense = Decimal(
        df_is.loc["業外損益合計", (yearQuarter, "金額")]
    )

    # 每股稅後盈餘(元)
    eps = Decimal(df_is.loc["每股稅後盈餘(元)", (yearQuarter, "金額")])
    print(f"每股稅後盈餘:{eps}")

    df_bs = get_fin_data(stockId, "balance_sheet")

    # 資產總額
    total_assets = Decimal(df_bs.loc["資產總額", (yearQuarter, "金額")])
    print(f"資產總額:{total_assets}")

    # 股東權益總額
    total_equity = Decimal(df_bs.loc["股東權益總額", (yearQuarter, "金額")])
    print(f"股東權益總額:{total_equity}")

    df_fr = get_fin_data(stockId, "financial_ratio")
    # print(df_fr)

    # 每股營業現金流量
    operating_cash_flow_per_share = Decimal(
        df_fr.loc["每股營業現金流量 (元)", yearQuarter]
    )
    print(f"每股營業現金流量:{operating_cash_flow_per_share}")

    # 每股自由現金流量
    free_cash_flow_per_share = Decimal(df_fr.loc["每股自由現金流量 (元)", yearQuarter])
    print(f"每股自由現金流量:{free_cash_flow_per_share}")

    # 財報評分 (100為滿分)
    financial_score = Decimal(df_fr.loc["財報評分 (100為滿分)", yearQuarter])
    print(f"財報評分:{financial_score}")

    # ---- 計算財務相關公式 ----

    # 毛利率 	    =（營業收入 - 營業成本） / 營業收入
    gross_profit = operating_revenue - operating_costs
    print(f"毛利:{gross_profit}")
    if operating_revenue == 0:
        gross_profit_margin = 0
    else:
        gross_profit_margin = (gross_profit / operating_revenue * 100).quantize(
            Decimal(".01"), rounding=ROUND_HALF_UP
        )
    print(f"毛利率:{gross_profit_margin}")

    # 營業利益率    =（營業收入 - 營業成本 - 營業費用） / 營業成本
    operating_profit = operating_revenue - operating_costs - operating_expenses
    print(f"營業利益:{operating_profit}")
    if operating_costs == 0:
        operating_profit_margin = 0
    else:
        operating_profit_margin = (operating_profit / operating_costs * 100).quantize(
            Decimal(".01"), rounding=ROUND_HALF_UP
        )
    print(f"營業利益率:{operating_profit_margin}")

    # 淨利率        =（毛利 – 營業費用 – 稅額） / 營業成本
    net_profit = gross_profit - operating_expenses - tax_expense
    print(f"淨利:{net_profit}")
    if operating_costs == 0:
        net_profit_margin = 0
    else:
        net_profit_margin = (net_profit / operating_costs * 100).quantize(
            Decimal(".01"), rounding=ROUND_HALF_UP
        )
    print(f"淨利率:{net_profit_margin}")

    # 稅前淨利率    = 營業利益 + 業外損益
    pre_tax_net_profit = operating_profit + non_operating_income_expense
    print(f"稅前淨利:{pre_tax_net_profit}")
    pre_tax_net_profit_margin = (pre_tax_net_profit / operating_revenue * 100).quantize(
        Decimal(".01"), rounding=ROUND_HALF_UP
    )
    print(f"稅前淨利率:{pre_tax_net_profit_margin}")

    # 稅後淨利率    = 稅前淨利 － 所得稅
    post_tax_net_profit = pre_tax_net_profit - tax_expense
    print(f"稅後淨利:{post_tax_net_profit}")
    post_tax_net_profit_margin = (
        post_tax_net_profit / operating_revenue * 100
    ).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
    print(f"稅後淨利率:{post_tax_net_profit_margin}")

    # 總資產週轉率  = 營業收入 / 總資產
    total_asset_turnover_ratio = (operating_revenue / total_assets).quantize(
        Decimal(".01"), rounding=ROUND_HALF_UP
    )
    print(f"總資產週轉率:{total_asset_turnover_ratio}")

    # 權益乘數(ROE) = 總資產 / 股東權益
    equity_multiplier = (total_assets / total_equity).quantize(
        Decimal(".01"), rounding=ROUND_HALF_UP
    )
    print(f"權益乘數:{equity_multiplier}")

    # 本業收益      = 營業利益 / 稅前淨利
    core_business_income_ratio = (operating_profit / pre_tax_net_profit * 100).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
    print(f"本業收益:{core_business_income_ratio}")
    
    # 构建包含所有指标的字典
    financial_metrics = {
        "毛利率": net_profit_margin,
        "營業利益率": operating_profit_margin,
        "ROE": total_equity,
        "稅前淨利率": pre_tax_net_profit_margin,
        "稅後淨利率": post_tax_net_profit_margin,
        "總資產週轉率": total_asset_turnover_ratio,
        "本業收益": core_business_income_ratio,
        "每股營業現金流量": operating_cash_flow_per_share,
        "每股自由現金流量": free_cash_flow_per_share,
        "財報評分": financial_score,
    }
    
    return pd.DataFrame([financial_metrics])
"""
盈餘再投資比率

公式：
當季長期投資和固定資產 - 4年前同期長期投資和固定資產 / 近16季稅後淨利總和

盈餘再投資比率代表：
企業近4年來在長期投資、固定資產的增加幅度，相對於近4年獲利總和大小。 
由於長期投資、固定資產的投資金額龐大，如果企業自身獲利不足以支應， 
則資金上將出現龐大缺口，財務壓力上升。 
因此盈餘再投資比率過高，則代表企業的投資金額遠高於自身獲利能力，財務和周轉風險上升。 
此比率為洪瑞泰在其著作"巴菲特選股魔法書"中所創，書中建議低於80%財務較為穩健；高於200%則企業財務風險過高，投資人應避開。
"""

# ------ 測試 ------
# data = get_fin_detail("8150")
# print(data)
