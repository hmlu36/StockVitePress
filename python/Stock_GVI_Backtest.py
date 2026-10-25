import yfinance as yf
import pandas as pd
import numpy as np
import twstock
import os
import sys
import datetime
import warnings

# 確保 Windows Console 支援 UTF-8 輸出
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

warnings.filterwarnings('ignore')

def get_stock_list(filename="stock_list.txt"):
    """讀取股票代號清單"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)

    if not os.path.exists(file_path):
        print(f"錯誤: 找不到檔案 {file_path}")
        return []

    stock_ids = []
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, header=None, dtype=str)
            stock_ids = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
        elif file_path.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.split('#')[0].strip()
                    if not line:
                        continue
                    parts = line.split(',')
                    for part in parts:
                        clean_part = part.strip().strip("'").strip('"').strip()
                        if clean_part:
                            stock_ids.append(clean_part)

        print(f"成功讀取 {len(stock_ids)} 檔股票代號。")
        return stock_ids
    except Exception as e:
        print(f"讀取清單發生異常: {e}")
        return []

def load_reference_data():
    """讀取彙整清單 CSV 作為輔助參考資料"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(os.path.dirname(current_dir), 'public', '彙整清單_整理版.csv')

        if not os.path.exists(csv_path):
            return pd.DataFrame()

        df = pd.read_csv(csv_path, dtype={'證券代號': str})
        df.columns = [c.strip() for c in df.columns]
        df['StockID'] = df['證券代號'].astype(str).str.strip()
        return df.set_index('StockID')
    except Exception as e:
        print(f"讀取參考資料失敗: {e}")
        return pd.DataFrame()

def get_tw_name(symbol):
    code = symbol.split('.')[0]
    try:
        return twstock.codes[code].name
    except Exception:
        return symbol

def parse_float(value, default=np.nan):
    if pd.isna(value):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        s = str(value).replace(',', '').replace('%', '').strip()
        if '/' in s:
            s = s.split('/')[0].strip()
        return float(s)
    except Exception:
        return default

# -------------------------------------------------------------
# 1. 調整後的選股與評價指標 (解決過度嚴苛與矛盾問題)
# -------------------------------------------------------------

def evaluate_fundamentals(info, ref_data=None):
    """
    客觀評估基本面與財務健康，避免一刀切淘汰金融股與代工股
    """
    res = {
        'ROE': np.nan,
        'PB': np.nan,
        'PE': np.nan,
        '殖利率': np.nan,
        '毛利率': np.nan,
        '營益率': np.nan,
        '負債比': np.nan,
        '流動比': np.nan,
        '自由現金流': np.nan,
        '產業': info.get('sector', '-'),
        '體質評等': '合格',
        '體質未達標原因': []
    }

    # 提取數值
    roe = info.get('returnOnEquity', np.nan)
    res['ROE'] = roe * 100 if roe and not pd.isna(roe) else parse_float(ref_data.get('ROE') if ref_data is not None else np.nan)
    
    pb = info.get('priceToBook', np.nan)
    res['PB'] = pb if pb and not pd.isna(pb) else parse_float(ref_data.get('淨值比') if ref_data is not None else np.nan)

    pe = info.get('trailingPE', np.nan)
    res['PE'] = pe if pe and not pd.isna(pe) else parse_float(ref_data.get('本益比') if ref_data is not None else np.nan)

    # 殖利率
    div_rate = info.get('dividendYield', np.nan)
    if div_rate and not pd.isna(div_rate):
        res['殖利率'] = div_rate * 100 if div_rate < 1 else div_rate
    elif ref_data is not None:
        raw_y = parse_float(ref_data.get('殖利率'))
        res['殖利率'] = raw_y / 100 if raw_y > 10 else raw_y

    # 毛利率與營益率
    gm = info.get('grossMargins', np.nan)
    res['毛利率'] = gm * 100 if gm and not pd.isna(gm) else parse_float(ref_data.get('毛利率') if ref_data is not None else np.nan)

    opm = info.get('operatingMargins', np.nan)
    res['營益率'] = opm * 100 if opm and not pd.isna(opm) else parse_float(ref_data.get('營業利益率') if ref_data is not None else np.nan)

    # 負債與流動性
    debt_equity = info.get('debtToEquity', np.nan)
    if debt_equity and not pd.isna(debt_equity):
        res['負債比'] = (debt_equity / (100 + debt_equity)) * 100
    res['流動比'] = info.get('currentRatio', np.nan)
    if res['流動比'] and not pd.isna(res['流動比']):
        res['流動比'] *= 100
    res['自由現金流'] = info.get('freeCashflow', np.nan)

    # --- 調整後的體質判定 (產業自適應) ---
    is_financial = 'Financial' in str(res['產業']) or '金控' in str(info.get('longName', ''))
    
    # 1. ROE 標準：一般股 >= 10%，金融股 >= 8%
    min_roe = 8.0 if is_financial else 10.0
    if pd.isna(res['ROE']) or res['ROE'] < min_roe:
        res['體質未達標原因'].append(f'ROE<{min_roe}%')

    # 2. 獲利性：本業獲利 (營益率 > 0，毛利率 > 0)
    if not is_financial:
        if pd.notna(res['營益率']) and res['營益率'] <= 0:
            res['體質未達標原因'].append('本業虧損')

    # 3. 負債比 (金融業不適用一般製造業負債比 < 65% 限制)
    if not is_financial:
        if pd.notna(res['負債比']) and res['負債比'] > 65:
            res['體質未達標原因'].append('負債比>65%')

    # 4. GVI 計算: (1/PB) * (1 + ROE)^5
    if pd.notna(res['PB']) and res['PB'] > 0 and pd.notna(res['ROE']) and res['ROE'] > 0:
        roe_r = res['ROE'] / 100
        res['GVI'] = (1 / res['PB']) * ((1 + roe_r) ** 5)
    else:
        res['GVI'] = np.nan

    if len(res['體質未達標原因']) == 0:
        res['體質評等'] = '優良'
    elif len(res['體質未達標原因']) == 1:
        res['體質評等'] = '尚可'
    else:
        res['體質評等'] = '偏弱'

    return res

# -------------------------------------------------------------
# 2. 技術時機點判定 (解決買賣矛盾與換手率盲點)
# -------------------------------------------------------------

def evaluate_timing_signals(df_p, shares_out=None):
    """
    評估即時技術面與時機點：
    - 修復換手率：非 <5% 跳過，而是標註是否流動性充裕或主力異常過熱
    - 區分「帶量突破買點」與「高檔出貨賣點」
    """
    res = {
        '收盤價': np.nan,
        '漲跌幅(%)': np.nan,
        '20MA': np.nan,
        '20日均量': np.nan,
        '量能倍數': np.nan,
        '換手率(%)': np.nan,
        '流動性狀態': '充足',
        '技術時機訊號': '盤整觀望',
        '時機類型': '觀望'
    }

    if df_p is None or df_p.empty or len(df_p) < 25:
        return res

    closes = pd.to_numeric(df_p['Close'], errors='coerce').dropna()
    volumes = pd.to_numeric(df_p['Volume'], errors='coerce').dropna()
    highs = pd.to_numeric(df_p['High'], errors='coerce').dropna()
    lows = pd.to_numeric(df_p['Low'], errors='coerce').dropna()
    opens = pd.to_numeric(df_p['Open'], errors='coerce').dropna()

    if len(closes) < 25 or len(volumes) < 25:
        return res

    c0 = closes.iloc[-1]
    c1 = closes.iloc[-2]
    o0 = opens.iloc[-1]
    h0 = highs.iloc[-1]
    v0 = volumes.iloc[-1]

    ma20_series = closes.rolling(20).mean()
    ma20 = ma20_series.iloc[-1]
    ma20_prev = ma20_series.iloc[-2]
    vol_ma20 = volumes.tail(20).mean()

    res['收盤價'] = round(c0, 2)
    res['漲跌幅(%)'] = round(((c0 - c1) / c1) * 100, 2) if c1 > 0 else 0
    res['20MA'] = round(ma20, 2)
    res['20日均量'] = vol_ma20
    vol_ratio = (v0 / vol_ma20) if vol_ma20 > 0 else 1.0
    res['量能倍數'] = round(vol_ratio, 2)

    # 換手率判斷
    if shares_out and shares_out > 0:
        turnover = (v0 / shares_out) * 100
        res['換手率(%)'] = round(turnover, 2)
        if turnover > 15.0:
            res['流動性狀態'] = '過熱(換手>15%)'
        elif (vol_ma20 / 1000) < 300:
            res['流動性狀態'] = '偏低(日均<300張)'
    else:
        if (vol_ma20 / 1000) < 300:
            res['流動性狀態'] = '偏低(日均<300張)'

    # 判斷近 20 日高低點
    high_20 = highs.iloc[-21:-1].max() if len(highs) >= 21 else highs.max()
    high_60 = highs.iloc[-61:-1].max() if len(highs) >= 61 else high_20

    # 判斷訊號
    is_ma20_up = ma20 >= ma20_prev
    is_above_ma20 = c0 >= ma20
    cross_above_ma20 = (c1 < ma20_prev) and (c0 >= ma20)
    is_breakout_20 = (c0 > high_20) and (c0 > o0) and (vol_ratio >= 1.25)

    # 出場/警戒判定
    cross_below_ma20 = (c1 >= ma20_prev) and (c0 < ma20)
    # 高檔爆量滯漲 (距離高點很近但留長黑K或長上影線)
    is_near_high_60 = (c0 >= high_60 * 0.95)
    is_exhaustion_vol = (vol_ratio >= 2.0) and ((c0 < o0) or ((h0 - c0) > (c0 - lows.iloc[-1])))

    if is_breakout_20:
        res['技術時機訊號'] = '帶量突破前高'
        res['時機類型'] = '買進'
    elif cross_above_ma20 and is_ma20_up and vol_ratio >= 1.0:
        res['技術時機訊號'] = '月線轉強黃金買點'
        res['時機類型'] = '買進'
    elif is_near_high_60 and is_exhaustion_vol:
        res['技術時機訊號'] = '高檔出貨爆量警戒'
        res['時機類型'] = '警戒/減碼'
    elif cross_below_ma20:
        res['技術時機訊號'] = '跌破月線出場'
        res['時機類型'] = '賣出'
    elif is_above_ma20:
        res['技術時機訊號'] = '月線上強勢整理'
        res['時機類型'] = '持有'
    else:
        res['技術時機訊號'] = '月線下弱勢盤整'
        res['時機類型'] = '觀望'

    return res

# -------------------------------------------------------------
# 3. 歷史回測引擎 (雙引擎：品質濾網 + 20MA突破進出場)
# -------------------------------------------------------------

def run_backtest_for_symbol(df_p, symbol, stop_loss_pct=0.08, take_profit_pct=0.35):
    """
    對單一股票進行歷史回測 (近 2-3 年數據)
    策略規則：
    - 進場：股價帶量突破月線 (Close > 20MA 且 Vol > 1.2 * Vol_MA20) 或突破 20 日高點，且 20MA 走平向上。
    - 出場：
      1. 硬停損：虧損達 -8% (stop_loss_pct) 無條件停損。
      2. 移動停利：連續兩日收盤跌破 20MA，或單日跌破 20MA 超過 2.5%。
      3. 估值/波段滿足：獲利達 +35% (take_profit_pct) 且跌破 5MA 時獲利了結。
    """
    res_summary = {
        '總交易次數': 0,
        '獲利次數': 0,
        '勝率(%)': 0.0,
        '累積報酬率(%)': 0.0,
        '年化報酬率(%)': 0.0,
        '買進持有報酬(%)': 0.0,
        '盈虧比': 0.0,
        '最大回撤(MDD %)': 0.0,
        '當前持股狀態': '空手觀望',
        '當前持股損益(%)': 0.0,
        '持股進場日': '-',
        '持股進場價': 0.0
    }

    if df_p is None or df_p.empty or len(df_p) < 60:
        return res_summary

    df = df_p.copy()
    for col in ['Close', 'High', 'Low', 'Open', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['Close', 'Volume'])

    if len(df) < 60:
        return res_summary

    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA5'] = df['Close'].rolling(5).mean()
    df['Vol_MA20'] = df['Volume'].rolling(20).mean()
    df['High20'] = df['High'].shift(1).rolling(20).max()

    in_position = False
    entry_price = 0.0
    entry_date = None
    trades = []
    equity_curve = [1.0]

    # 從第 25 天開始回測
    for i in range(25, len(df)):
        curr_date = df.index[i]
        c0 = df['Close'].iloc[i]
        c_prev = df['Close'].iloc[i - 1]
        v0 = df['Volume'].iloc[i]
        ma20 = df['MA20'].iloc[i]
        ma20_prev = df['MA20'].iloc[i - 1]
        ma5 = df['MA5'].iloc[i]
        vol_ma20 = df['Vol_MA20'].iloc[i]
        h20 = df['High20'].iloc[i]

        if not in_position:
            # 買進訊號判定
            is_breakout = (c0 > h20) and (v0 >= vol_ma20 * 1.2) if pd.notna(h20) else False
            cross_up = (c_prev < ma20_prev) and (c0 > ma20) and (ma20 >= ma20_prev) and (v0 >= vol_ma20 * 1.1)

            if is_breakout or cross_up:
                in_position = True
                entry_price = c0
                entry_date = curr_date
        else:
            # 賣出判定
            gain_pct = (c0 - entry_price) / entry_price

            # 1. 硬停損
            is_stop_loss = gain_pct <= -stop_loss_pct

            # 2. 移動停利 (跌破 20MA)
            c_prev2 = df['Close'].iloc[i - 1]
            ma20_prev2 = df['MA20'].iloc[i - 1]
            drop_below_ma20 = (c0 < ma20 * 0.975) or ((c0 < ma20) and (c_prev2 < ma20_prev2))

            # 3. 達標鎖利
            is_take_profit = (gain_pct >= take_profit_pct) and (c0 < ma5)

            if is_stop_loss or drop_below_ma20 or is_take_profit or (i == len(df) - 1):
                # 執行平倉
                exit_price = c0
                ret = (exit_price - entry_price) / entry_price
                reason = '停損' if is_stop_loss else ('目標停利' if is_take_profit else '均線出場')
                
                # 若為最後一天且未真正觸發賣出，標記持有中
                if i == len(df) - 1 and not (is_stop_loss or drop_below_ma20 or is_take_profit):
                    res_summary['當前持股狀態'] = '持有中'
                    res_summary['當前持股損益(%)'] = round(gain_pct * 100, 2)
                    res_summary['持股進場日'] = entry_date.strftime('%Y-%m-%d') if hasattr(entry_date, 'strftime') else str(entry_date)[:10]
                    res_summary['持股進場價'] = round(entry_price, 2)
                else:
                    trades.append({
                        'entry_date': entry_date,
                        'exit_date': curr_date,
                        'return': ret,
                        'reason': reason
                    })
                    equity_curve.append(equity_curve[-1] * (1.0 + ret))
                    in_position = False

    # 計算統計指標
    bh_return = (df['Close'].iloc[-1] - df['Close'].iloc[25]) / df['Close'].iloc[25]
    res_summary['買進持有報酬(%)'] = round(bh_return * 100, 2)

    if trades:
        res_summary['總交易次數'] = len(trades)
        wins = [t['return'] for t in trades if t['return'] > 0]
        losses = [abs(t['return']) for t in trades if t['return'] < 0]

        res_summary['獲利次數'] = len(wins)
        res_summary['勝率(%)'] = round((len(wins) / len(trades)) * 100, 2)

        cum_ret = equity_curve[-1] - 1.0
        res_summary['累積報酬率(%)'] = round(cum_ret * 100, 2)

        # 年化 (以天數換算，約 250 交易日/年)
        trading_years = max((len(df) - 25) / 250, 0.5)
        cagr = ((1.0 + cum_ret) ** (1.0 / trading_years) - 1.0) if (1.0 + cum_ret) > 0 else -1.0
        res_summary['年化報酬率(%)'] = round(cagr * 100, 2)

        total_gain = sum(wins)
        total_loss = sum(losses)
        res_summary['盈虧比'] = round(total_gain / total_loss, 2) if total_loss > 0 else (99.0 if total_gain > 0 else 0.0)

        # MDD 計算
        eq_series = pd.Series(equity_curve)
        rolling_max = eq_series.cummax()
        drawdown = (eq_series - rolling_max) / rolling_max
        res_summary['最大回撤(MDD %)'] = round(abs(drawdown.min()) * 100, 2)

    return res_summary

# -------------------------------------------------------------
# 4. 主執行流程與 CSV 匯出
# -------------------------------------------------------------

def main():
    print("=" * 60)
    print("[INIT] 啟動 GVI + 三因子動態回測與買賣時機點系統")
    print("=" * 60)

    stock_list = get_stock_list()
    if not stock_list:
        print("未取得股票代號，終止執行。")
        return

    ref_df = load_reference_data()

    print(f"正在批量下載 {len(stock_list)} 檔股票歷史數據 (近 3 年)...")
    try:
        history_data = yf.download(
            stock_list,
            period="3y",
            group_by='ticker',
            threads=True,
            auto_adjust=True,
            progress=False
        )
    except Exception as e:
        print(f"批量下載失敗: {e}")
        history_data = pd.DataFrame()

    signals_report = []
    performance_report = []

    for idx, symbol in enumerate(stock_list):
        try:
            stock_id = symbol.split('.')[0]
            tw_name = get_tw_name(symbol)
            print(f"[{idx+1}/{len(stock_list)}] 分析中: {symbol} ({tw_name})...")

            ticker = yf.Ticker(symbol)
            try:
                info = ticker.info or {}
            except Exception:
                info = {}

            ref_data = ref_df.loc[stock_id] if (ref_df is not None and stock_id in ref_df.index) else None

            # 1. 取得歷史價量
            if not history_data.empty:
                df_p = history_data[symbol] if len(stock_list) > 1 else history_data
            else:
                df_p = ticker.history(period="3y")

            # 2. 評估基本面與體質
            fund = evaluate_fundamentals(info, ref_data)

            # 3. 評估即時技術時機點
            shares_out = info.get('sharesOutstanding', None)
            timing = evaluate_timing_signals(df_p, shares_out)

            # 4. 執行策略歷史回測
            bt = run_backtest_for_symbol(df_p, symbol)

            # 5. 綜合決策建議 (結合基本面安全邊際與技術時機)
            decision = "觀望"
            if fund['體質評等'] in ['優良', '尚可']:
                if timing['時機類型'] == '買進':
                    decision = "強烈買進 (雙因子共振)"
                elif bt['當前持股狀態'] == '持有中':
                    decision = f"續抱中 (波段+{bt['當前持股損益(%)']}%)"
                elif timing['時機類型'] == '持有':
                    decision = "多頭持股/拉回買"
                elif timing['時機類型'] == '賣出':
                    decision = "跌破月線出場"
                elif timing['時機類型'] == '警戒/減碼':
                    decision = "高檔爆量分批獲利"
                else:
                    decision = "基本面良好，靜待買點"
            else:
                if timing['時機類型'] == '賣出':
                    decision = "弱勢賣出"
                else:
                    reason_str = ';'.join(fund['體質未達標原因']) if fund['體質未達標原因'] else '未達標'
                    decision = f"濾網未過 ({reason_str})"

            # 組織「即時時機訊號清單」
            signals_report.append({
                '證券代號': symbol,
                '證券名稱': tw_name,
                '收盤價': timing['收盤價'],
                '漲跌幅': f"{timing['漲跌幅(%)']}%",
                '決策建議': decision,
                '技術時機點': timing['技術時機訊號'],
                '體質評等': fund['體質評等'],
                'GVI指標': round(fund['GVI'], 2) if pd.notna(fund['GVI']) else "-",
                '本益比': round(fund['PE'], 2) if pd.notna(fund['PE']) else "-",
                '淨值比': round(fund['PB'], 2) if pd.notna(fund['PB']) else "-",
                'ROE': f"{fund['ROE']:.1f}%" if pd.notna(fund['ROE']) else "-",
                '殖利率': f"{fund['殖利率']:.2f}%" if pd.notna(fund['殖利率']) else "-",
                '20MA': timing['20MA'],
                '量能倍數': f"{timing['量能倍數']}x",
                '換手率': f"{timing['換手率(%)']}%" if pd.notna(timing['換手率(%)']) else "-",
                '流動性狀態': timing['流動性狀態'],
                '回測持股狀態': bt['當前持股狀態'],
                '產業': fund['產業']
            })

            # 組織「歷史回測排行榜」
            performance_report.append({
                '證券代號': symbol,
                '證券名稱': tw_name,
                '累積報酬率': f"{bt['累積報酬率(%)']}%",
                '年化報酬率(CAGR)': f"{bt['年化報酬率(%)']}%",
                '勝率': f"{bt['勝率(%)']}%",
                '盈虧比': bt['盈虧比'],
                '最大回撤(MDD)': f"{bt['最大回撤(MDD %)']}%",
                '總交易次數': bt['總交易次數'],
                '買進持有報酬': f"{bt['買進持有報酬(%)']}%",
                '當前狀態': bt['當前持股狀態'],
                '當前損益': f"{bt['當前持股損益(%)']}%" if bt['當前持股狀態'] == '持有中' else "-",
                '進場日期': bt['持股進場日']
            })

        except Exception as e:
            print(f"處理 {symbol} 異常: {e}")

    # 轉為 DataFrame
    df_signals = pd.DataFrame(signals_report)
    df_perf = pd.DataFrame(performance_report)

    # 排序
    if not df_signals.empty:
        # 優先將買進與持有排在前面
        prio_map = {
            '強烈買進 (雙因子共振)': 1,
            '多頭持股/拉回買': 2,
            '續抱中': 3,
            '基本面良好，靜待買點': 4,
            '高檔爆量分批獲利': 5,
            '跌破月線出場': 6,
            '弱勢賣出': 7
        }
        df_signals['sort_key'] = df_signals['決策建議'].apply(lambda x: next((v for k, v in prio_map.items() if k in str(x)), 99))
        df_signals = df_signals.sort_values(by=['sort_key', 'GVI指標'], ascending=[True, False]).drop(columns=['sort_key'])

    if not df_perf.empty:
        # 依年化報酬率數值排序
        def parse_pct_for_sort(x):
            try:
                return float(str(x).replace('%', ''))
            except:
                return -999.0
        df_perf['cagr_num'] = df_perf['年化報酬率(CAGR)'].apply(parse_pct_for_sort)
        df_perf = df_perf.sort_values(by='cagr_num', ascending=False).drop(columns=['cagr_num'])

    # 輸出目錄
    current_dir = os.path.dirname(os.path.abspath(__file__))
    public_dir = os.path.join(os.path.dirname(current_dir), 'public')
    os.makedirs(public_dir, exist_ok=True)

    sig_path = os.path.join(public_dir, 'Stock_Backtest_Signals.csv')
    perf_path = os.path.join(public_dir, 'Stock_Backtest_Performance.csv')

    df_signals.to_csv(sig_path, index=False, encoding='utf-8-sig')
    df_perf.to_csv(perf_path, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 60)
    print(f"[SUCCESS] 輸出即時買賣時機儀表板: {sig_path}")
    print(f"[SUCCESS] 輸出歷史回測績效排行榜: {perf_path}")
    print("=" * 60)

    print("\n【即時強烈買進 / 關鍵時機點 (前10檔)】")
    if not df_signals.empty:
        print(df_signals[['證券代號', '證券名稱', '收盤價', '決策建議', '技術時機點', '體質評等']].head(10).to_string(index=False))

    print("\n【回測年化績效表現最佳 (前10檔)】")
    if not df_perf.empty:
        print(df_perf[['證券代號', '證券名稱', '年化報酬率(CAGR)', '勝率', '盈虧比', '最大回撤(MDD)']].head(10).to_string(index=False))

if __name__ == '__main__':
    main()
