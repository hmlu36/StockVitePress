import yfinance as yf
import pandas as pd
import numpy as np
import twstock
import time
import os
import datetime


def get_stock_list(filename="stock_list.txt"):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)

    print(f"正在讀取檔案路徑: {file_path}")

    if not os.path.exists(file_path):
        print(f"錯誤: 找不到檔案，請確認檔案是否位於 {current_dir}")
        return []

    stock_ids = []
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, header=None, dtype=str)
            stock_ids = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
        elif file_path.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    # 去除註解
                    line = line.split('#')[0].strip()
                    if not line:
                        continue
                    # 分割並清理
                    parts = line.split(',')
                    for part in parts:
                        clean_part = part.strip().strip("'").strip('"').strip()
                        if clean_part:
                            stock_ids.append(clean_part)

        print(f"成功讀取 {len(stock_ids)} 檔股票代號。")
        return stock_ids
    except Exception as e:
        print(f"讀取過程發生例外狀況: {e}")
        return []


def load_reference_data():
    """讀取彙整清單 CSV 作為參考資料"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 假設 CSV 在 public 資料夾
        csv_path = os.path.join(os.path.dirname(current_dir), 'public', '彙整清單_整理版.csv')

        if not os.path.exists(csv_path):
            print(f"提醒: 找不到參考資料 {csv_path}")
            return pd.DataFrame()

        print(f"正在讀取參考資料: {csv_path}")
        df = pd.read_csv(csv_path, dtype={'證券代號': str})
        # 清理欄位名稱
        df.columns = [c.strip() for c in df.columns]
        # 建立索引
        df['StockID'] = df['證券代號'].astype(str).str.strip()
        return df.set_index('StockID')
    except Exception as e:
        print(f"讀取參考資料失敗: {e}")
        return pd.DataFrame()


def get_tw_name(symbol):
    code = symbol.split('.')[0]
    try:
        return twstock.codes[code].name
    except:
        return symbol


def parse_float(value, default=np.nan):
    """嘗試將字串或數字轉換為 float"""
    if pd.isna(value):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        # 處理可能的字串格式，例如 "12.3%" 或 "1,234"
        s = str(value).replace(',', '').replace('%', '').strip()
        # 處理 "20.5 / 0" 這種格式，取前面
        if '/' in s:
            s = s.split('/')[0].strip()
        return float(s)
    except:
        return default


def calculate_min_pe_5y(ticker, history_data, symbol):
    """計算近五年(或四年)最低本益比"""
    try:
        # 取得年度財報中的 EPS
        fin = ticker.financials
        if fin.empty:
            return np.nan

        if 'Basic EPS' not in fin.index:
            return np.nan

        eps_series = fin.loc['Basic EPS']
        # 確保是數值
        eps_series = pd.to_numeric(eps_series, errors='coerce').dropna()

        if eps_series.empty:
            return np.nan

        min_pes = []

        # 取得股價歷史
        df_p = history_data[symbol] if not history_data.empty and symbol in history_data else pd.DataFrame()
        if df_p.empty:
            # 若批量抓取沒有，嘗試個別抓取
            df_p = ticker.history(period="5y")

        if df_p.empty:
            return np.nan

        df_p.index = pd.to_datetime(df_p.index)

        for date, eps in eps_series.items():
            year = date.year
            # 取得該年度的股價數據
            mask = df_p.index.year == year
            year_prices = df_p.loc[mask]

            if not year_prices.empty and eps > 0:
                min_price = year_prices['Close'].min()
                min_pe = min_price / eps
                min_pes.append(min_pe)

        if min_pes:
            return min(min_pes)
        return np.nan
    except Exception as e:
        return np.nan


def fetch_stock_data(stock_list, ref_df=None):
    print(f"正在抓取 {len(stock_list)} 檔股票資料...")
    result_list = []

    # 批量下載歷史股價（擴展至 5 年以計算最低本益比）
    try:
        # 注意: 5y 資料量較大，若股票多可能會慢。
        history_data = yf.download(stock_list, period="5y", group_by='ticker', threads=True)
    except:
        history_data = pd.DataFrame()

    for symbol in stock_list:
        try:
            stock_id = symbol.split('.')[0]

            # 取得參考資料 (如果有的話)
            ref_data = None
            if ref_df is not None and stock_id in ref_df.index:
                ref_data = ref_df.loc[stock_id]

            ticker = yf.Ticker(symbol)
            # 嘗試取得 info，若失敗則為空字典
            try:
                info = ticker.info
            except:
                info = {}

            # --- 1. 基礎價格 ---
            # 優先使用 yfinance 即時資料，再回退到參考資料
            close = np.nan

            # 先嘗試從 yfinance info 取得
            close = info.get('currentPrice') or info.get('previousClose')

            # 若 info 沒有，嘗試從歷史資料取得
            if (pd.isna(close) or close == 0) and not history_data.empty:
                try:
                    df_price = history_data[symbol] if len(stock_list) > 1 else history_data
                    valid_closes = df_price['Close'].dropna()
                    if not valid_closes.empty:
                        close = valid_closes.iloc[-1]
                except:
                    pass

            # 最後才使用參考資料（可能是舊的）
            if (pd.isna(close) or close == 0) and ref_data is not None:
                close = parse_float(ref_data.get('收盤'))

            # --- 2. 估值指標 ---
            pe = np.nan
            pb = np.nan
            roe = np.nan

            if ref_data is not None:
                pe = parse_float(ref_data.get('本益比'))
                pb = parse_float(ref_data.get('淨值比'))
                roe = parse_float(ref_data.get('ROE'))

            if pd.isna(pe):
                pe = info.get('trailingPE', np.nan)
            if pd.isna(pb):
                pb = info.get('priceToBook', np.nan)
            if pd.isna(roe):
                roe_yf = info.get('returnOnEquity', np.nan)
                roe = roe_yf * 100 if roe_yf else np.nan

            # --- 3. 20 日報酬率 ---
            r_20d = np.nan
            if not history_data.empty:
                try:
                    df_p = history_data[symbol] if len(stock_list) > 1 else history_data
                    if len(df_p) >= 20:
                        current_close = close if (close and not pd.isna(close)) else df_p['Close'].iloc[-1]
                        price_20d_ago = df_p['Close'].iloc[-20]
                        r_20d = (current_close - price_20d_ago) / price_20d_ago
                except:
                    pass

            # --- 4. GVI 計算 ---
            roe_ratio = roe / 100 if (roe and roe > 1) else roe
            gvi = np.nan
            if pb and pb > 0 and roe_ratio and roe_ratio > 0:
                gvi = (1/pb * (1+roe_ratio)**5)
            elif ref_data is not None:
                gvi = parse_float(ref_data.get('GVI指標'))

            # --- 5. 殖利率 ---
            div_yield = np.nan
            if ref_data is not None:
                raw_yield = parse_float(ref_data.get('殖利率'))
                if not pd.isna(raw_yield):
                    div_yield = raw_yield / 100

            if pd.isna(div_yield):
                div_rate = info.get('dividendRate', 0)
                div_yield = (div_rate / close * 100) if (div_rate and close) else 0

            # --- 6. 獲利能力 (毛利率, 營業利益率, 稅後淨利率, 稅前淨利率) ---
            gross_margin = np.nan
            op_margin = np.nan
            net_margin = np.nan
            pretax_margin = np.nan
            revenue_growth = np.nan

            if ref_data is not None:
                gross_margin = parse_float(ref_data.get('毛利率'))
                op_margin = parse_float(ref_data.get('營業利益率'))
                net_margin = parse_float(ref_data.get('稅後淨利率'))

            # 從 Info 獲取基礎數據
            if pd.isna(gross_margin):
                gross_margin = info.get('grossMargins', 0) * 100
            if pd.isna(op_margin):
                op_margin = info.get('operatingMargins', 0) * 100
            if pd.isna(net_margin):
                net_margin = info.get('profitMargins', 0) * 100

            revenue_growth = info.get('revenueGrowth', 0) * 100

            # 嘗試從 Financials 計算 稅前淨利率 & 補強其他數據
            try:
                fin = ticker.financials
                if not fin.empty:
                    # 稅前淨利率 = Pretax Income / Total Revenue
                    if 'Pretax Income' in fin.index and 'Total Revenue' in fin.index:
                        pretax_income = fin.loc['Pretax Income'].iloc[0]
                        total_revenue = fin.loc['Total Revenue'].iloc[0]
                        if total_revenue != 0:
                            pretax_margin = (pretax_income / total_revenue) * 100
            except:
                pass

            # 本業比例 (營業利益率 / 稅前淨利率)
            core_ratio = np.nan
            denominator = pretax_margin if (pretax_margin and not pd.isna(pretax_margin)) else net_margin

            if not pd.isna(op_margin) and not pd.isna(denominator) and denominator > 0:
                core_ratio = (op_margin / denominator * 100)

            # --- 7. PE 區間 & 5年最低 PE ---
            pe_range_str = "-"
            pe_min_5y = np.nan

            # 計算 5年最低 PE
            pe_min_5y = calculate_min_pe_5y(ticker, history_data, symbol)

            if ref_data is not None and '本益比區間' in ref_data:
                pe_range_str = str(ref_data['本益比區間'])

            if pe_range_str == "-" or pe_range_str == "nan":
                eps = info.get('trailingEps')
                if not history_data.empty and eps and eps > 0 and close:
                    try:
                        df_p = history_data[symbol] if len(stock_list) > 1 else history_data
                        high_y = df_p['Close'].tail(250).max()
                        low_y = df_p['Close'].tail(250).min()
                        pe_cur = close / eps
                        pe_min = low_y / eps
                        pe_max = high_y / eps
                        pe_range_str = f"{pe_cur:.1f} [{pe_min:.1f}-{pe_max:.1f}]"
                    except:
                        pass

            # --- 8. 籌碼 (外資持股) ---
            foreign_pct = np.nan
            if ref_data is not None:
                foreign_pct = parse_float(ref_data.get('外資持股(%)'))

            if pd.isna(foreign_pct):
                foreign_pct = info.get('heldPercentInstitutions', 0) * 100
                if foreign_pct > 100:
                    foreign_pct = np.nan

            vol_lots = 0
            if ref_data is not None:
                vol_lots = parse_float(ref_data.get('張數'), 0)

            if vol_lots == 0:
                vol_shares = info.get('volume', 0)
                vol_lots = int(vol_shares / 1000) if vol_shares else 0

            # --- 9. 收盤日期 ---
            close_date = ""
            if not history_data.empty:
                try:
                    df_p = history_data[symbol] if len(stock_list) > 1 else history_data
                    if not df_p.empty:
                        # 找出實際有收盤價的最後一筆日期
                        # 過濾掉 NaN 值，取最後一筆有效資料的日期
                        valid_closes = df_p['Close'].dropna()
                        if not valid_closes.empty:
                            last_date = valid_closes.index[-1]
                            close_date = last_date.strftime('%Y-%m-%d')
                except:
                    pass

            if not close_date and info.get('regularMarketTime'):
                try:
                    ts = info.get('regularMarketTime')
                    close_date = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                except:
                    pass

            result_list.append({
                '證券代號': symbol,
                '證券名稱': get_tw_name(symbol),
                '收盤': close,
                '收盤日期': close_date,
                'GVI指標': gvi,
                '20日報酬率': r_20d,
                '殖利率': div_yield,
                '本益比': pe,
                '淨值比': pb,
                'ROE': roe,
                '外資持股(%)': foreign_pct,
                '張數': vol_lots,
                '毛利率': gross_margin,
                '營業利益率': op_margin,
                '稅後淨利率': net_margin,
                '稅前淨利率': pretax_margin,
                '本業比例': core_ratio,
                '營收成長率': revenue_growth,
                '近五年最低本益比': pe_min_5y,
                '本益比區間': pe_range_str
            })

        except Exception as e:
            print(f"Error {symbol}: {e}")

    return pd.DataFrame(result_list)


def calculate_scores(df):
    """計算三因子評分"""
    # 確保數值型態
    numeric_cols = ['淨值比', 'ROE', '20日報酬率']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df_valid = df[(df['淨值比'] > 0) & (df['ROE'] > 0)].copy()

    if df_valid.empty:
        return df

    # 百分位排名（0-100）
    df_valid['ROE_Score'] = df_valid['ROE'].rank(pct=True) * 100
    df_valid['PB_Score'] = (1 / df_valid['淨值比']).rank(pct=True) * 100
    df_valid['R_Score'] = df_valid['20日報酬率'].rank(pct=True) * 100 if '20日報酬率' in df_valid.columns else 0

    # 三因子等權重評分
    df_valid['三因子評分'] = df_valid['ROE_Score'] * 0.33 + df_valid['PB_Score'] * 0.33 + df_valid['R_Score'] * 0.34

    return df_valid


def format_and_export(df):
    # 找出最常見的收盤日期 (Mode)
    date_str = ""
    if '收盤日期' in df.columns and not df['收盤日期'].dropna().empty:
        try:
            date_str = df['收盤日期'].mode()[0]
        except:
            pass

    # 決定收盤欄位名稱
    close_col_name = f'收盤 ({date_str})' if date_str else '收盤'

    # 若有日期，將 '收盤' 欄位重新命名
    if date_str and '收盤' in df.columns:
        df = df.rename(columns={'收盤': close_col_name})

    cols = [
        '證券代號', '證券名稱', close_col_name, 'GVI指標', '三因子評分', '20日報酬率',
        '殖利率', '本益比', '近五年最低本益比', '淨值比', 'ROE',
        '外資持股(%)', '張數', '毛利率', '營業利益率', '稅後淨利率', '稅前淨利率', '本業比例', '營收成長率', '本益比區間'
    ]

    # 只保留存在的欄位
    df = df[[c for c in cols if c in df.columns]].copy()

    # 格式化
    def fmt_f2(x): return f"{x:.2f}" if isinstance(x, (int, float)) and not pd.isna(x) else "-"
    def fmt_pct(x): return f"{x:.2f}%" if isinstance(x, (int, float)) and not pd.isna(x) else "-"

    if close_col_name in df.columns:
        df[close_col_name] = df[close_col_name].apply(lambda x: f"{x:.1f}" if isinstance(x, (int, float)) and not pd.isna(x) else "-")

    df['GVI指標'] = df['GVI指標'].apply(fmt_f2)
    df['三因子評分'] = df['三因子評分'].apply(fmt_f2)
    df['淨值比'] = df['淨值比'].apply(fmt_f2)
    df['本益比'] = df['本益比'].apply(fmt_f2)
    df['近五年最低本益比'] = df['近五年最低本益比'].apply(fmt_f2)

    pct_cols = ['20日報酬率', '殖利率', 'ROE', '外資持股(%)', '毛利率', '營業利益率', '稅後淨利率', '稅前淨利率', '本業比例', '營收成長率']
    for c in pct_cols:
        if c in df.columns:
            df[c] = df[c].apply(fmt_pct)

    df['張數'] = df['張數'].apply(lambda x: f"{int(x):,}" if isinstance(x, (int, float)) and not pd.isna(x) else "-")

    return df


if __name__ == "__main__":
    my_stocks = get_stock_list()

    # 讀取參考資料
    ref_df = load_reference_data()

    raw_df = fetch_stock_data(my_stocks, ref_df)

    if not raw_df.empty:
        scored_df = calculate_scores(raw_df)
        final_df = format_and_export(scored_df)

        # 依三因子評分排序
        if '三因子評分' in final_df.columns:
            final_df = final_df.sort_values('三因子評分', ascending=False)

        print(final_df.head(20).to_string(index=False))

        # 設定輸出路徑到 public 資料夾
        current_dir = os.path.dirname(os.path.abspath(__file__))
        public_dir = os.path.join(os.path.dirname(current_dir), 'public')

        # 確保 public 資料夾存在
        if not os.path.exists(public_dir):
            os.makedirs(public_dir)

        output_path = os.path.join(public_dir, 'Stock_GVI_ThreeFactor.csv')
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n已輸出至 {output_path}")
    else:
        print("查無資料")
