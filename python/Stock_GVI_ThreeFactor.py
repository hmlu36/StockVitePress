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


def calculate_volume_analysis(history_data, symbol, stock_list):
    """
    計算成交量相關指標，用於偵測「低檔盤整後成交量大增」的技術型態

    回傳:
    - vol_ma5: 5日成交量移動平均
    - vol_ma20: 20日成交量移動平均
    - vol_ratio: 當日成交量 / 20日均量 (成交量放大倍數)
    - vol_breakout: 是否發生成交量突破 (當日成交量 > 均量2倍)
    - low_consolidation: 是否在低檔盤整 (股價接近近20日低點 + 最近均量萎縮)
    - vol_signal: 綜合訊號 ("量能放大突破", "低檔盤整", "正常", 等)
    """
    result = {
        'vol_ma5': np.nan,
        'vol_ma20': np.nan,
        'vol_ratio': np.nan,
        'vol_breakout': False,
        'low_consolidation': False,
        'vol_signal': '-'
    }

    try:
        if history_data.empty:
            return result

        # 取得股價歷史
        df_p = history_data[symbol] if len(stock_list) > 1 else history_data

        if df_p.empty or len(df_p) < 20:
            return result

        # 取得成交量數據
        if 'Volume' not in df_p.columns:
            return result

        volumes = df_p['Volume'].dropna()
        closes = df_p['Close'].dropna()

        if len(volumes) < 20 or len(closes) < 20:
            return result

        # 計算均量
        vol_ma5 = volumes.tail(5).mean()
        vol_ma20 = volumes.tail(20).mean()
        current_vol = volumes.iloc[-1]

        result['vol_ma5'] = vol_ma5
        result['vol_ma20'] = vol_ma20

        # 計算成交量放大倍數
        if vol_ma20 > 0:
            result['vol_ratio'] = current_vol / vol_ma20

        # 判斷成交量突破 (當日成交量 > 均量2倍)
        if vol_ma20 > 0 and current_vol > vol_ma20 * 2:
            result['vol_breakout'] = True

        # 判斷低檔盤整
        # 條件1: 股價接近近20日低點 (在低點5%範圍內)
        # 條件2: 最近5日均量 < 20日均量 (量能萎縮)
        low_20d = closes.tail(20).min()
        current_close = closes.iloc[-1]
        high_20d = closes.tail(20).max()

        price_near_low = (current_close - low_20d) / low_20d < 0.05 if low_20d > 0 else False
        # 另一種判斷: 股價在近20日震幅的下半部
        price_in_lower_half = (current_close - low_20d) < (high_20d - low_20d) * 0.4 if high_20d > low_20d else False

        vol_contraction = vol_ma5 < vol_ma20 * 0.8  # 近5日均量 < 20日均量的80%

        if (price_near_low or price_in_lower_half) and vol_contraction:
            result['low_consolidation'] = True

        # 綜合訊號判斷
        if result['low_consolidation'] and result['vol_breakout']:
            result['vol_signal'] = '★低檔量增突破'  # 最佳買點訊號
        elif result['vol_breakout']:
            result['vol_signal'] = '量能放大'
        elif result['low_consolidation']:
            result['vol_signal'] = '低檔盤整'
        elif result['vol_ratio'] and result['vol_ratio'] > 1.5:
            result['vol_signal'] = '量能增加'
        elif result['vol_ratio'] and result['vol_ratio'] < 0.5:
            result['vol_signal'] = '量能萎縮'
        else:
            result['vol_signal'] = '正常'

    except Exception as e:
        pass

    return result


def fetch_stock_data(stock_list, ref_df=None):
    print(f"正在抓取 {len(stock_list)} 檔股票資料...")
    result_list = []

    # 批量下載歷史股價（擴展至 3 年以計算最低本益比）
    try:
        # 注意: 3y 資料量較大，若股票多可能會慢。
        history_data = yf.download(stock_list, period="3y", group_by='ticker', threads=True)
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

            # --- 10. 財務健康指標 (現金流穩定、低負債) ---
            # 自由現金流 (Free Cash Flow)
            free_cash_flow = np.nan
            # 營業現金流 (Operating Cash Flow)
            operating_cash_flow = np.nan
            # 負債比率 (Debt Ratio) = 總負債 / 總資產
            debt_ratio = np.nan
            # 流動比率 (Current Ratio) = 流動資產 / 流動負債
            current_ratio = np.nan
            # 速動比率 (Quick Ratio) = (流動資產-存貨) / 流動負債
            quick_ratio = np.nan
            # 現金流量比 (Cash Flow Ratio) = 營業現金流 / 稅後淨利
            cash_flow_ratio = np.nan
            # 產業類別
            sector = info.get('sector', '')
            industry = info.get('industry', '')

            # 從 yfinance 抓取現金流與資產負債表資料
            try:
                # 現金流量表
                cf = ticker.cashflow
                if not cf.empty:
                    if 'Free Cash Flow' in cf.index:
                        free_cash_flow = cf.loc['Free Cash Flow'].iloc[0]
                    if 'Operating Cash Flow' in cf.index:
                        operating_cash_flow = cf.loc['Operating Cash Flow'].iloc[0]

                # 資產負債表
                bs = ticker.balance_sheet
                if not bs.empty:
                    total_assets = bs.loc['Total Assets'].iloc[0] if 'Total Assets' in bs.index else np.nan
                    total_debt = bs.loc['Total Debt'].iloc[0] if 'Total Debt' in bs.index else np.nan
                    total_liab = bs.loc['Total Liabilities Net Minority Interest'].iloc[0] if 'Total Liabilities Net Minority Interest' in bs.index else np.nan

                    current_assets = bs.loc['Current Assets'].iloc[0] if 'Current Assets' in bs.index else np.nan
                    current_liab = bs.loc['Current Liabilities'].iloc[0] if 'Current Liabilities' in bs.index else np.nan
                    inventory = bs.loc['Inventory'].iloc[0] if 'Inventory' in bs.index else 0

                    # 計算負債比率
                    if not pd.isna(total_liab) and not pd.isna(total_assets) and total_assets > 0:
                        debt_ratio = (total_liab / total_assets) * 100

                    # 計算流動比率
                    if not pd.isna(current_assets) and not pd.isna(current_liab) and current_liab > 0:
                        current_ratio = (current_assets / current_liab) * 100

                    # 計算速動比率
                    if not pd.isna(current_assets) and not pd.isna(current_liab) and current_liab > 0:
                        quick_assets = current_assets - (inventory if not pd.isna(inventory) else 0)
                        quick_ratio = (quick_assets / current_liab) * 100

                # 計算現金流量比 (營業現金流 / 稅後淨利)
                income_stmt = ticker.financials
                if not income_stmt.empty and not pd.isna(operating_cash_flow):
                    net_income = income_stmt.loc['Net Income'].iloc[0] if 'Net Income' in income_stmt.index else np.nan
                    if not pd.isna(net_income) and net_income > 0:
                        cash_flow_ratio = (operating_cash_flow / net_income) * 100

            except Exception as e:
                pass

            # 從 info 補充（若上面沒抓到）
            if pd.isna(debt_ratio):
                debt_to_equity = info.get('debtToEquity', np.nan)
                if not pd.isna(debt_to_equity):
                    # 負債權益比轉負債比率: D/E -> D/(D+E) = 1/(1+E/D)
                    debt_ratio = (debt_to_equity / (100 + debt_to_equity)) * 100

            if pd.isna(current_ratio):
                current_ratio = info.get('currentRatio', np.nan)
                if not pd.isna(current_ratio):
                    current_ratio = current_ratio * 100

            if pd.isna(quick_ratio):
                quick_ratio = info.get('quickRatio', np.nan)
                if not pd.isna(quick_ratio):
                    quick_ratio = quick_ratio * 100

            if pd.isna(free_cash_flow):
                free_cash_flow = info.get('freeCashflow', np.nan)

            # --- 11. 成交量分析 (低檔盤整+量能放大) ---
            vol_analysis = calculate_volume_analysis(history_data, symbol, stock_list)

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
                '本益比區間': pe_range_str,
                # --- 新增: 財務健康指標 ---
                '自由現金流': free_cash_flow,
                '負債比率': debt_ratio,
                '流動比率': current_ratio,
                '速動比率': quick_ratio,
                '現金流量比': cash_flow_ratio,
                '產業': sector,
                '細產業': industry,
                # --- 新增: 成交量分析 ---
                '5日均量': vol_analysis['vol_ma5'],
                '20日均量': vol_analysis['vol_ma20'],
                '量能倍數': vol_analysis['vol_ratio'],
                '量能訊號': vol_analysis['vol_signal']
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


def calculate_financial_health_score(df):
    """
    計算財務健康評分
    評分邏輯：
    - 現金流正向 (+20分): 自由現金流 > 0
    - 低負債 (+20分): 負債比率 < 50%
    - 高流動性 (+15分): 流動比率 > 150%
    - 本業強健 (+15分): 本業比例 > 60%
    - 獲利穩定 (+15分): 現金流量比 > 80%
    - 本益比合理 (+15分): PE < 15 或 PE < 近5年最低PE
    """
    df = df.copy()

    # 確保數值型態
    health_cols = ['自由現金流', '負債比率', '流動比率', '本業比例', '現金流量比', '本益比', '近五年最低本益比']
    for col in health_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 初始化評分
    df['財務健康評分'] = 0.0

    # 1. 現金流正向 (+20分)
    if '自由現金流' in df.columns:
        df.loc[df['自由現金流'] > 0, '財務健康評分'] += 20

    # 2. 低負債 (+20分): 負債比率 < 50%
    if '負債比率' in df.columns:
        df.loc[df['負債比率'] < 50, '財務健康評分'] += 20

    # 3. 高流動性 (+15分): 流動比率 > 150%
    if '流動比率' in df.columns:
        df.loc[df['流動比率'] > 150, '財務健康評分'] += 15

    # 4. 本業強健 (+15分): 本業比例 > 60%
    if '本業比例' in df.columns:
        df.loc[df['本業比例'] > 60, '財務健康評分'] += 15

    # 5. 獲利穩定 (+15分): 現金流量比 > 80%
    if '現金流量比' in df.columns:
        df.loc[df['現金流量比'] > 80, '財務健康評分'] += 15

    # 6. 本益比合理 (+15分): PE < 15 或 PE < 近5年最低PE
    if '本益比' in df.columns:
        pe_reasonable = (df['本益比'] < 15) & (df['本益比'] > 0)
        if '近五年最低本益比' in df.columns:
            pe_undervalued = (df['本益比'] < df['近五年最低本益比']) & (df['本益比'] > 0)
            df.loc[pe_reasonable | pe_undervalued, '財務健康評分'] += 15
        else:
            df.loc[pe_reasonable, '財務健康評分'] += 15

    return df


def filter_quality_stocks(df, strict=False):
    """
    篩選高品質股票 (現金流穩定、低負債、真正賺錢)

    篩選條件 (寬鬆模式):
    - 本益比 < 20 (避開高本益比夢想股)
    - 負債比率 < 60%
    - 本業比例 > 50%

    篩選條件 (嚴格模式 strict=True):
    - 本益比 < 15
    - 負債比率 < 50%
    - 流動比率 > 150%
    - 本業比例 > 60%
    - 自由現金流 > 0
    - ROE > 10%
    """
    df = df.copy()

    # 確保數值型態
    filter_cols = ['本益比', '負債比率', '流動比率', '本業比例', '自由現金流', 'ROE']
    for col in filter_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 建立篩選標籤欄位
    df['通過品質篩選'] = True
    df['排除原因'] = ''

    if strict:
        # 嚴格模式
        # 1. 本益比 < 15
        if '本益比' in df.columns:
            fail_pe = (df['本益比'] >= 15) | (df['本益比'] <= 0)
            df.loc[fail_pe, '通過品質篩選'] = False
            df.loc[fail_pe, '排除原因'] += '高PE;'

        # 2. 負債比率 < 50%
        if '負債比率' in df.columns:
            fail_debt = df['負債比率'] >= 50
            df.loc[fail_debt, '通過品質篩選'] = False
            df.loc[fail_debt, '排除原因'] += '高負債;'

        # 3. 流動比率 > 150%
        if '流動比率' in df.columns:
            fail_current = df['流動比率'] <= 150
            df.loc[fail_current, '通過品質篩選'] = False
            df.loc[fail_current, '排除原因'] += '低流動;'

        # 4. 本業比例 > 60%
        if '本業比例' in df.columns:
            fail_core = df['本業比例'] <= 60
            df.loc[fail_core, '通過品質篩選'] = False
            df.loc[fail_core, '排除原因'] += '低本業;'

        # 5. 自由現金流 > 0
        if '自由現金流' in df.columns:
            fail_fcf = df['自由現金流'] <= 0
            df.loc[fail_fcf, '通過品質篩選'] = False
            df.loc[fail_fcf, '排除原因'] += '負現金流;'

        # 6. ROE > 10%
        if 'ROE' in df.columns:
            fail_roe = df['ROE'] <= 10
            df.loc[fail_roe, '通過品質篩選'] = False
            df.loc[fail_roe, '排除原因'] += '低ROE;'
    else:
        # 寬鬆模式
        # 1. 本益比 < 20 (避開高本益比夢想股)
        if '本益比' in df.columns:
            fail_pe = (df['本益比'] >= 20) | (df['本益比'] <= 0)
            df.loc[fail_pe, '通過品質篩選'] = False
            df.loc[fail_pe, '排除原因'] += '高PE;'

        # 2. 負債比率 < 60%
        if '負債比率' in df.columns:
            fail_debt = df['負債比率'] >= 60
            df.loc[fail_debt, '通過品質篩選'] = False
            df.loc[fail_debt, '排除原因'] += '高負債;'

        # 3. 本業比例 > 50%
        if '本業比例' in df.columns:
            fail_core = df['本業比例'] <= 50
            df.loc[fail_core, '通過品質篩選'] = False
            df.loc[fail_core, '排除原因'] += '低本業;'

    return df


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
        '證券代號', '證券名稱', close_col_name, 'GVI指標', '三因子評分', '財務健康評分', '通過品質篩選',
        '20日報酬率',
        # --- 新增: 成交量分析欄位 ---
        '5日均量', '20日均量', '量能倍數', '量能訊號',
        '殖利率', '本益比', '近五年最低本益比', '淨值比', 'ROE',
        '外資持股(%)', '張數', '毛利率', '營業利益率', '稅後淨利率', '稅前淨利率', '本業比例', '營收成長率',
        '自由現金流', '負債比率', '流動比率', '速動比率', '現金流量比',
        '本益比區間', '產業', '細產業', '排除原因'
    ]

    # 只保留存在的欄位
    df = df[[c for c in cols if c in df.columns]].copy()

    # 格式化
    def fmt_f2(x): return f"{x:.2f}" if isinstance(x, (int, float)) and not pd.isna(x) else "-"
    def fmt_pct(x): return f"{x:.2f}%" if isinstance(x, (int, float)) and not pd.isna(x) else "-"

    def fmt_cash(x):
        if pd.isna(x) or not isinstance(x, (int, float)):
            return "-"
        # 以億為單位顯示
        return f"{x/1e8:.1f}億" if abs(x) >= 1e8 else f"{x/1e6:.1f}百萬"

    def fmt_volume(x):
        """將成交量(股)轉為張數顯示"""
        if pd.isna(x) or not isinstance(x, (int, float)):
            return "-"
        lots = x / 1000  # 1張 = 1000股
        if lots >= 10000:
            return f"{lots/10000:.1f}萬張"
        elif lots >= 1000:
            return f"{lots/1000:.1f}千張"
        else:
            return f"{lots:.0f}張"

    if close_col_name in df.columns:
        df[close_col_name] = df[close_col_name].apply(lambda x: f"{x:.1f}" if isinstance(x, (int, float)) and not pd.isna(x) else "-")

    df['GVI指標'] = df['GVI指標'].apply(fmt_f2)
    if '三因子評分' in df.columns:
        df['三因子評分'] = df['三因子評分'].apply(fmt_f2)
    if '財務健康評分' in df.columns:
        df['財務健康評分'] = df['財務健康評分'].apply(fmt_f2)
    df['淨值比'] = df['淨值比'].apply(fmt_f2)
    df['本益比'] = df['本益比'].apply(fmt_f2)
    df['近五年最低本益比'] = df['近五年最低本益比'].apply(fmt_f2)

    # 格式化量能倍數
    if '量能倍數' in df.columns:
        df['量能倍數'] = df['量能倍數'].apply(lambda x: f"{x:.2f}x" if isinstance(x, (int, float)) and not pd.isna(x) else "-")

    # 格式化均量
    if '5日均量' in df.columns:
        df['5日均量'] = df['5日均量'].apply(fmt_volume)
    if '20日均量' in df.columns:
        df['20日均量'] = df['20日均量'].apply(fmt_volume)

    pct_cols = ['20日報酬率', '殖利率', 'ROE', '外資持股(%)', '毛利率', '營業利益率', '稅後淨利率', '稅前淨利率', '本業比例', '營收成長率', '負債比率', '流動比率', '速動比率', '現金流量比']
    for c in pct_cols:
        if c in df.columns:
            df[c] = df[c].apply(fmt_pct)

    if '自由現金流' in df.columns:
        df['自由現金流'] = df['自由現金流'].apply(fmt_cash)

    df['張數'] = df['張數'].apply(lambda x: f"{int(x):,}" if isinstance(x, (int, float)) and not pd.isna(x) else "-")

    return df


if __name__ == "__main__":
    my_stocks = get_stock_list()

    # 讀取參考資料
    ref_df = load_reference_data()

    raw_df = fetch_stock_data(my_stocks, ref_df)

    if not raw_df.empty:
        # 1. 計算三因子評分
        scored_df = calculate_scores(raw_df)

        # 2. 計算財務健康評分
        scored_df = calculate_financial_health_score(scored_df)

        # 3. 品質篩選 (可選擇 strict=True 為嚴格模式)
        scored_df = filter_quality_stocks(scored_df, strict=True)

        # 4. 格式化輸出
        final_df = format_and_export(scored_df)

        # 依財務健康評分 + 三因子評分排序
        if '財務健康評分' in final_df.columns:
            final_df = final_df.sort_values(
                by=['通過品質篩選', '財務健康評分', '三因子評分'],
                ascending=[False, False, False]
            )
        elif '三因子評分' in final_df.columns:
            final_df = final_df.sort_values('三因子評分', ascending=False)

        # 顯示通過篩選的股票
        passed_df = final_df[final_df['通過品質篩選'] == True] if '通過品質篩選' in final_df.columns else final_df
        print("=== 通過品質篩選的股票 ===")
        print(passed_df.head(20).to_string(index=False))

        # 設定輸出路徑到 public 資料夾
        current_dir = os.path.dirname(os.path.abspath(__file__))
        public_dir = os.path.join(os.path.dirname(current_dir), 'public')

        # 確保 public 資料夾存在
        if not os.path.exists(public_dir):
            os.makedirs(public_dir)

        # 輸出完整清單
        output_path = os.path.join(public_dir, 'Stock_GVI_ThreeFactor.csv')
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n已輸出完整清單至 {output_path}")

        # 輸出通過篩選的清單
        if '通過品質篩選' in final_df.columns:
            quality_output_path = os.path.join(public_dir, 'Stock_Quality_Filtered.csv')
            passed_df.to_csv(quality_output_path, index=False, encoding='utf-8-sig')
            print(f"已輸出品質篩選清單至 {quality_output_path} (共 {len(passed_df)} 檔)")
    else:
        print("查無資料")
