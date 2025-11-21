import yfinance as yf
import pandas as pd
import numpy as np
import twstock
import time
import os
import re
import datetime

# --- 1. 讀取股票清單 ---


def load_stock_list(file_path: str) -> list:
    if not os.path.exists(file_path):
        print(f"找不到 {file_path}，請確保檔案存在。")
        return []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        # Regex 提取 xxxx.TW 或 xxxx.TWO
        pattern = r"(\d{4}\.TW(?:O)?)"
        stock_ids = list(dict.fromkeys(re.findall(pattern, content)))
        print(f"成功讀取 {len(stock_ids)} 檔股票。")
        return stock_ids
    except Exception as e:
        print(f"讀取失敗: {e}")
        return []

# --- 2. 取得中文名稱與產業 ---


def get_stock_info_tw(symbol):
    code = symbol.split('.')[0]
    name = symbol
    try:
        if code in twstock.codes:
            name = twstock.codes[code].name
    except:
        pass
    return name

# --- 3. 抓取基本面與損益表 ---


def get_fundamental_data(stock_list):
    data = []
    print("正在抓取基本面、股利與損益表資料...")

    for symbol in stock_list:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            # 基本資料
            name = get_stock_info_tw(symbol)
            # 產業別 (yfinance 有時是英文，可顯示 Sector)
            industry = info.get('sector', 'N/A')
            if industry == 'Technology':
                industry = '電子'  # 簡單翻譯範例
            if industry == 'Financial Services':
                industry = '金融'

            roe = info.get('returnOnEquity', np.nan)
            pb = info.get('priceToBook', np.nan)
            eps = info.get('trailingEps', np.nan)

            # 股利 (現金)
            cash_div = info.get('dividendRate', 0)
            if cash_div is None:
                cash_div = 0

            # 股票股利 (yfinance 無法穩定取得，暫設為 0)
            stock_div = 0

            # 本業與業外佔比
            core_ratio = np.nan
            non_core_ratio = np.nan

            try:
                qs = stock.quarterly_income_stmt
                if not qs.empty:
                    recent = qs.iloc[:, 0]
                    op_income = recent.get('Operating Income')
                    pretax = recent.get('Pretax Income')

                    if op_income and pretax and pretax != 0:
                        core_ratio = op_income / pretax
                        non_core_ratio = 1 - core_ratio
            except:
                pass

            data.append({
                'Symbol': symbol,
                'Name': name,
                'Industry': industry,
                'ROE': roe,
                'PB': pb,
                'EPS': eps,
                'Cash_Div': cash_div,
                'Stock_Div': stock_div,
                'Core_Ratio': core_ratio,
                'Non_Core_Ratio': non_core_ratio
            })
            time.sleep(0.3)  # 避免過快

        except Exception as e:
            print(f"略過 {symbol}: {e}")

    return pd.DataFrame(data)

# --- 4. 抓取技術面、均線與 PE區間 ---


def get_technical_data(stock_list):
    print("正在批量計算技術指標 (均線、動能、PE區間)...")
    try:
        # 抓取過去 1 年 (用於計算一年內最高最低 PE) + 緩衝
        df_all = yf.download(stock_list, period="300d",
                             group_by='ticker', auto_adjust=True, threads=True)
        tech_list = []

        for symbol in stock_list:
            try:
                # 處理單檔/多檔結構差異
                df = df_all if len(stock_list) == 1 else df_all[symbol]
                df = df.dropna()

                if len(df) < 60:
                    continue

                close = df['Close'].iloc[-1]

                # 均線
                ma5 = df['Close'].rolling(5).mean().iloc[-1]
                ma60 = df['Close'].rolling(60).mean().iloc[-1]

                # 動能 (月漲幅)
                price_1m = df['Close'].iloc[-20] if len(
                    df) >= 20 else df['Close'].iloc[0]
                momentum = (close - price_1m) / price_1m

                # PE 區間計算 (需要近一年 High/Low)
                # 取近 250 天
                df_year = df['Close'].tail(250)
                high_year = df_year.max()
                low_year = df_year.min()

                tech_list.append({
                    'Symbol': symbol,
                    'Close': close,
                    'MA5': ma5,
                    'MA60': ma60,
                    'Momentum': momentum,
                    'High_Year': high_year,
                    'Low_Year': low_year
                })
            except:
                pass

        return pd.DataFrame(tech_list)
    except Exception as e:
        print(f"技術資料抓取失敗: {e}")
        return pd.DataFrame()

# --- 5. 綜合計算與格式化 ---


def calculate_final_metrics(df):
    # 先移除必要數據缺失者
    df = df.dropna(subset=['Close', 'ROE', 'PB'])

    # 1. 計算殖利率
    df['Yield'] = (df['Cash_Div'] / df['Close']) * 100

    # 2. 計算 GVI 指標 = (1/PB) * (1 + ROE)^5
    # 這裡 ROE 原始數據通常是小數 (0.15)，若 yfinance 給的是 0.15
    df['GVI'] = (1 / df['PB']) * ((1 + df['ROE']) ** 5)

    # 3. 計算 PE 區間字串
    # Min PE = Low / EPS, Max PE = High / EPS, Cur PE = Close / EPS
    # 需處理 EPS <= 0 的情況
    def calc_pe_str(row):
        if pd.isna(row['EPS']) or row['EPS'] <= 0:
            return "N/A (虧損)"
        cur_pe = row['Close'] / row['EPS']
        min_pe = row['Low_Year'] / row['EPS']
        max_pe = row['High_Year'] / row['EPS']
        return f"{cur_pe:.1f} [{min_pe:.1f}-{max_pe:.1f}]"

    df['PE_Range_Str'] = df.apply(calc_pe_str, axis=1)

    # 4. 綜合評分 (ROE + PB + 動能 + GVI)
    # 權重分配：ROE(30%), PB(20%), Mom(20%), GVI(30%)
    r_roe = df['ROE'].rank(pct=True)
    r_pb = (1/df['PB']).rank(pct=True)
    r_mom = df['Momentum'].rank(pct=True)
    r_gvi = df['GVI'].rank(pct=True)

    df['Score_Raw'] = (r_roe * 0.3) + (r_pb * 0.2) + \
        (r_mom * 0.2) + (r_gvi * 0.3)
    df['Final_Score'] = (df['Score_Raw'] / df['Score_Raw'].max()) * 100

    # 5. 排序
    df = df.sort_values(by='Final_Score', ascending=False)

    # 6. 欄位對應與格式化
    cols_map = {
        'Symbol': '股票代號',
        'Name': '公司名稱',
        'Close': '收盤價',
        'PE_Range_Str': 'PE區間(現價[低-高])',
        'Momentum': '動能(月漲)',
        'MA5': '5日均線',
        'MA60': '60日均線',
        'ROE': 'ROE(%)',
        'PB': '股價淨值比',
        'Industry': '產業別',
        'Cash_Div': '現金股利',
        'Stock_Div': '股票股利',  # 僅佔位
        'Yield': '殖利率(%)',
        'Core_Ratio': '本業佔比',
        'Non_Core_Ratio': '業外佔比',
        'Final_Score': '綜合評分',
        'GVI': 'GVI指標'
    }

    df_out = df.rename(columns=cols_map)

    # 數值美化
    def fmt_pct(x): return "-" if pd.isna(x) else f"{x:.2%}"
    def fmt_num(x): return "-" if pd.isna(x) else f"{x:.2f}"

    df_out['收盤價'] = df_out['收盤價'].apply(lambda x: f"{x:.2f}")
    df_out['動能(月漲)'] = df_out['動能(月漲)'].apply(fmt_pct)
    df_out['ROE(%)'] = df_out['ROE(%)'].apply(fmt_pct)
    df_out['本業佔比'] = df_out['本業佔比'].apply(fmt_pct)
    df_out['業外佔比'] = df_out['業外佔比'].apply(fmt_pct)
    df_out['殖利率(%)'] = df_out['殖利率(%)'].apply(lambda x: f"{x:.2f}%")
    df_out['綜合評分'] = df_out['綜合評分'].apply(lambda x: f"{x:.1f}")
    df_out['GVI指標'] = df_out['GVI指標'].apply(fmt_num)
    df_out['5日均線'] = df_out['5日均線'].apply(fmt_num)
    df_out['60日均線'] = df_out['60日均線'].apply(fmt_num)
    df_out['現金股利'] = df_out['現金股利'].apply(fmt_num)
    df_out['股價淨值比'] = df_out['股價淨值比'].apply(fmt_num)

    target_cols = list(cols_map.values())
    return df_out[target_cols]


# --- 主程式 ---
if __name__ == "__main__":
    # 確保同目錄下有 stock_list.txt
    stocks = load_stock_list('stock_list.txt')
    if stocks:
        df_fund = get_fundamental_data(stocks)
        df_tech = get_technical_data(stocks)

        if not df_fund.empty and not df_tech.empty:
            df_merge = pd.merge(df_fund, df_tech, on='Symbol')
            final_report = calculate_final_metrics(df_merge)

            print("\n=== 分析完成 (前30名) ===")
            print(final_report.head(30).to_string(index=False))

            final_report.to_csv(
                f"Stock_Full_Report_{datetime.date.today()}.csv", index=False, encoding='utf-8-sig')
            print("\n檔案已輸出為 CSV。")
