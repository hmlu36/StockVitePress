import yfinance as yf
import pandas as pd
import numpy as np
import twstock
import time
import os


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


def get_tw_name(symbol):
    code = symbol.split('.')[0]
    try:
        return twstock.codes[code].name
    except:
        return symbol


def fetch_stock_data(stock_list):
    print(f"正在抓取 {len(stock_list)} 檔股票資料...")
    result_list = []

    # 批量下載歷史股價（擴展至 6 個月以計算 20 日報酬率）
    try:
        history_data = yf.download(stock_list, period="6mo", group_by='ticker', threads=True)
    except:
        history_data = pd.DataFrame()

    for symbol in stock_list:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info

            # 基礎價格
            close = info.get('currentPrice') or info.get('previousClose')
            if not close and not history_data.empty:
                try:
                    df_price = history_data[symbol] if len(stock_list) > 1 else history_data
                    close = df_price['Close'].iloc[-1]
                except:
                    pass

            # 估值指標
            pe = info.get('trailingPE', np.nan)
            pb = info.get('priceToBook', np.nan)
            roe = info.get('returnOnEquity', np.nan)

            # 計算 20 日報酬率
            r_20d = np.nan
            if not history_data.empty:
                try:
                    df_p = history_data[symbol] if len(stock_list) > 1 else history_data
                    if len(df_p) >= 20:
                        price_20d_ago = df_p['Close'].iloc[-20]
                        r_20d = (close - price_20d_ago) / price_20d_ago
                except:
                    pass

            # GVI 計算
            gvi = (1/pb * (1+roe)**5) if (pb and pb > 0 and roe and roe > 0) else np.nan

            # 殖利率
            div_rate = info.get('dividendRate', 0)
            div_yield = (div_rate / close * 100) if (div_rate and close) else 0

            # 獲利能力
            gross_margin = info.get('grossMargins', 0) * 100
            op_margin = info.get('operatingMargins', 0) * 100
            net_margin = info.get('profitMargins', 0) * 100

            # 本業比例 (營業利益率 / 稅後淨利率)
            core_ratio = (op_margin / net_margin * 100) if (net_margin and net_margin > 0) else np.nan

            # PE 區間
            pe_range_str = "-"
            eps = info.get('trailingEps')
            if not history_data.empty and eps and eps > 0:
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

            # 籌碼
            foreign_pct = info.get('heldPercentInstitutions', 0) * 100
            if foreign_pct > 100:
                foreign_pct = np.nan

            vol_shares = info.get('volume', 0)
            vol_lots = int(vol_shares / 1000) if vol_shares else 0

            result_list.append({
                '證券代號': symbol,
                '證券名稱': get_tw_name(symbol),
                '收盤': close,
                'GVI指標': gvi,
                '20日報酬率': r_20d,
                '殖利率': div_yield,
                '本益比': pe,
                '淨值比': pb,
                'ROE': roe * 100 if roe else np.nan,
                '外資持股(%)': foreign_pct,
                '張數': vol_lots,
                '毛利率': gross_margin,
                '營業利益率': op_margin,
                '稅後淨利率': net_margin,
                '本業比例': core_ratio,
                '本益比區間': pe_range_str
            })

            time.sleep(0.1)

        except Exception as e:
            print(f"Error {symbol}: {e}")

    return pd.DataFrame(result_list)


def calculate_scores(df):
    """計算三因子評分"""
    df_valid = df[(df['淨值比'] > 0) & (df['ROE'] > 0)].copy()

    # 百分位排名（0-100）
    df_valid['ROE_Score'] = df_valid['ROE'].rank(pct=True) * 100
    df_valid['PB_Score'] = (1 / df_valid['淨值比']).rank(pct=True) * 100
    df_valid['R_Score'] = df_valid['20日報酬率'].rank(pct=True) * 100 if '20日報酬率' in df_valid.columns else 0

    # 三因子等權重評分
    df_valid['三因子評分'] = df_valid['ROE_Score'] * 0.33 + df_valid['PB_Score'] * 0.33 + df_valid['R_Score'] * 0.34

    return df_valid


def format_and_export(df):
    cols = [
        '證券代號', '證券名稱', '收盤', 'GVI指標', '三因子評分', '20日報酬率',
        '殖利率', '本益比', '淨值比', 'ROE',
        '外資持股(%)', '張數', '毛利率', '營業利益率', '稅後淨利率', '本業比例', '本益比區間'
    ]

    df = df[[c for c in cols if c in df.columns]].copy()

    # 格式化
    def fmt_f2(x): return f"{x:.2f}" if isinstance(x, (int, float)) and not pd.isna(x) else "-"
    def fmt_pct(x): return f"{x:.2f}%" if isinstance(x, (int, float)) and not pd.isna(x) else "-"

    df['收盤'] = df['收盤'].apply(lambda x: f"{x:.1f}" if x else "-")
    df['GVI指標'] = df['GVI指標'].apply(fmt_f2)
    df['三因子評分'] = df['三因子評分'].apply(fmt_f2)
    df['淨值比'] = df['淨值比'].apply(fmt_f2)
    df['本益比'] = df['本益比'].apply(fmt_f2)

    pct_cols = ['20日報酬率', '殖利率', 'ROE', '外資持股(%)', '毛利率', '營業利益率', '稅後淨利率', '本業比例']
    for c in pct_cols:
        if c in df.columns:
            df[c] = df[c].apply(fmt_pct)

    df['張數'] = df['張數'].apply(lambda x: f"{x:,}" if isinstance(x, int) else "-")

    return df


if __name__ == "__main__":
    my_stocks = get_stock_list()

    raw_df = fetch_stock_data(my_stocks)

    if not raw_df.empty:
        scored_df = calculate_scores(raw_df)
        final_df = format_and_export(scored_df)

        # 依三因子評分排序
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
