"""
Stock_GVI_Unified.py - 全方位 GVI 估值 × TDCC 主力籌碼 × 波段量化回測 整合分析引擎
整合原 Stock_GVI_ThreeFactor.py, Stock_GVI_SmartMoney.py, Stock_GVI_Backtest.py 之核心演算法。
只進行一次 TDCC 與 yfinance 批次下載，大幅縮短執行時間與降低限流風險，並統一產出所有報表。
"""

import os
import sys
import datetime
import warnings
import ssl
import urllib.request
import numpy as np
import pandas as pd
import yfinance as yf
import twstock

# 確保 Windows Console 支援 UTF-8 輸出
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

warnings.filterwarnings('ignore')

# -------------------------------------------------------------
# 1. 基礎工具與資料讀取
# -------------------------------------------------------------

def get_stock_list(filename="stock_list.txt"):
    """讀取股票代號清單"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)

    if not os.path.exists(file_path):
        print(f"[警告] 找不到檔案: {file_path}")
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

        print(f"[讀取] 成功讀取 {len(stock_ids)} 檔股票代號清單。")
        return stock_ids
    except Exception as e:
        print(f"[錯誤] 讀取股票清單異常: {e}")
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
        print(f"[提示] 讀取彙整清單失敗: {e}")
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
# 2. TDCC 集保主力大戶數據
# -------------------------------------------------------------

def fetch_tdcc_shareholder_distribution():
    """從 TDCC 取得每週全市場股東持股分級資料"""
    url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
    print("正在連線 TDCC 集保資料庫下載最新股東持股分級資料...")

    try:
        context = ssl._create_unverified_context()
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=context, timeout=20) as response:
            df = pd.read_csv(response)

        df.columns = [c.strip() for c in df.columns]
        date_col = [c for c in df.columns if '日期' in c][0]
        code_col = [c for c in df.columns if '代號' in c or '證券' in c][0]
        level_col = [c for c in df.columns if '分級' in c][0]
        person_col = [c for c in df.columns if '人數' in c][0]
        ratio_col = [c for c in df.columns if '比例' in c or '比率' in c][0]

        df[code_col] = df[code_col].astype(str).str.strip()
        df = df[df[code_col].str.match(r'^\d{4}$')]
        df[level_col] = pd.to_numeric(df[level_col], errors='coerce')
        df[person_col] = pd.to_numeric(df[person_col], errors='coerce').fillna(0)
        df[ratio_col] = pd.to_numeric(df[ratio_col], errors='coerce').fillna(0)

        data_date = str(df[date_col].iloc[0])
        print(f"[TDCC] 集保資料下載成功！結算日期: {data_date}，涵蓋個股數: {df[code_col].nunique()}")

        records = []
        for stock_id, g in df.groupby(code_col):
            # 排除分級 16/17 等合計列
            g_valid = g[(g[level_col] >= 1) & (g[level_col] <= 15)]
            total_holders = g_valid[person_col].sum() if not g_valid.empty else g[person_col].sum()

            level_15 = g[g[level_col] == 15]
            pct_over_1000 = level_15[ratio_col].sum() if not level_15.empty else 0.0
            holders_over_1000 = int(level_15[person_col].sum()) if not level_15.empty else 0

            # 400張以上大戶: 分級 12~15
            level_400plus = g[(g[level_col] >= 12) & (g[level_col] <= 15)]
            pct_over_400 = level_400plus[ratio_col].sum() if not level_400plus.empty else 0.0

            # 散戶: 分級 1~9
            level_retail = g[(g[level_col] >= 1) & (g[level_col] <= 9)]
            pct_under_100 = level_retail[ratio_col].sum() if not level_retail.empty else 0.0
            retail_holders = int(level_retail[person_col].sum()) if not level_retail.empty else 0

            if pct_over_1000 >= 60 or pct_over_400 >= 70:
                concentration_tier = "極高 (主力掌控)"
            elif pct_over_1000 >= 45 or pct_over_400 >= 55:
                concentration_tier = "良好 (籌碼集中)"
            elif pct_over_1000 >= 30:
                concentration_tier = "普通 (結構均衡)"
            else:
                concentration_tier = "偏低 (散戶為主)"

            records.append({
                'StockID': stock_id,
                '集保日期': data_date,
                '總股東人數': int(total_holders),
                '千張大戶比例(%)': round(float(pct_over_1000), 2),
                '千張大戶人數': holders_over_1000,
                '400張大戶比例(%)': round(float(pct_over_400), 2),
                '100張以下散戶比例(%)': round(float(pct_under_100), 2),
                '散戶人數': retail_holders,
                '籌碼集中等級': concentration_tier
            })

        return pd.DataFrame(records).set_index('StockID')
    except Exception as e:
        print(f"[錯誤] TDCC 集保資料抓取失敗: {e}")
        return pd.DataFrame()

# -------------------------------------------------------------
# 3. 演算法模組：成交量分析、五大守則、最低PE
# -------------------------------------------------------------

def calculate_min_pe_5y(ticker, history_data, symbol):
    """計算近 5 年最低本益比"""
    try:
        fin = ticker.financials
        if fin.empty or 'Basic EPS' not in fin.index:
            return np.nan

        eps_series = pd.to_numeric(fin.loc['Basic EPS'], errors='coerce').dropna()
        if eps_series.empty:
            return np.nan

        df_p = history_data[symbol] if (not history_data.empty and symbol in history_data) else pd.DataFrame()
        if df_p.empty:
            return np.nan

        df_p.index = pd.to_datetime(df_p.index)
        min_pes = []

        for date, eps in eps_series.items():
            year = date.year
            mask = df_p.index.year == year
            year_prices = df_p.loc[mask]

            if not year_prices.empty and eps > 0:
                min_price = year_prices['Close'].min()
                min_pe = min_price / eps
                min_pes.append(min_pe)

        if min_pes:
            return min(min_pes)
        return np.nan
    except Exception:
        return np.nan

def calculate_volume_analysis(df_p):
    """低檔盤整後成交量突破訊號"""
    result = {
        'vol_ma5': np.nan,
        'vol_ma20': np.nan,
        'vol_ratio': np.nan,
        'vol_breakout': False,
        'low_consolidation': False,
        'vol_signal': '-'
    }

    if df_p is None or df_p.empty or len(df_p) < 20:
        return result

    try:
        volumes = pd.to_numeric(df_p['Volume'], errors='coerce').dropna()
        closes = pd.to_numeric(df_p['Close'], errors='coerce').dropna()

        if len(volumes) < 20 or len(closes) < 20:
            return result

        vol_ma5 = volumes.tail(5).mean()
        vol_ma20 = volumes.tail(20).mean()
        current_vol = volumes.iloc[-1]

        if current_vol == 0:
            return result

        result['vol_ma5'] = vol_ma5
        result['vol_ma20'] = vol_ma20

        if vol_ma20 > 0:
            result['vol_ratio'] = current_vol / vol_ma20

        if vol_ma20 > 0 and current_vol > vol_ma20 * 2:
            result['vol_breakout'] = True

        current_close = closes.iloc[-1]

        # 策略 A: 底部起漲
        is_bottom_breakout = False
        if len(closes) >= 125:
            past_period = closes.iloc[-125:-5]
            if not past_period.empty:
                box_high = past_period.max()
                box_low = past_period.min()
                if box_low > 0:
                    box_amplitude = (box_high - box_low) / box_low
                    is_consolidated = box_amplitude < 0.45
                    is_breakout = current_close > box_high
                    is_fresh_breakout = current_close <= box_high * 1.15
                    if is_consolidated and is_breakout and is_fresh_breakout and result['vol_breakout']:
                        is_bottom_breakout = True

        # 策略 B: 低檔佈局
        low_20d = closes.tail(20).min()
        high_20d = closes.tail(20).max()
        price_near_low = (current_close - low_20d) / low_20d < 0.05 if low_20d > 0 else False
        price_in_lower_half = (current_close - low_20d) < (high_20d - low_20d) * 0.4 if high_20d > low_20d else False
        vol_contraction = vol_ma5 < vol_ma20 * 0.8

        if (price_near_low or price_in_lower_half) and vol_contraction:
            result['low_consolidation'] = True

        if is_bottom_breakout:
            result['vol_signal'] = '★底部起漲'
        elif result['low_consolidation'] and result['vol_breakout']:
            result['vol_signal'] = '★低檔量增'
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
    except Exception:
        pass

    return result

def check_five_trading_rules(df_p, info):
    """五大交易紀律守則"""
    result = {
        '漲破7沒鎖漲停': False,
        '低開3站不回紅': False,
        '連漲3天': False,
        '高檔爆量': False,
        '換手率小於5': False,
        '換手率': np.nan,
        '五大守則建議': '-'
    }
    try:
        if df_p is None or df_p.empty or len(df_p) < 4:
            return result

        today = df_p.iloc[-1]
        yest = df_p.iloc[-2]
        c0 = pd.to_numeric(today['Close'], errors='coerce')
        h0 = pd.to_numeric(today['High'], errors='coerce')
        o0 = pd.to_numeric(today['Open'], errors='coerce')
        v0 = pd.to_numeric(today['Volume'], errors='coerce')
        c1 = pd.to_numeric(yest['Close'], errors='coerce')

        if pd.isna(c0) or pd.isna(c1) or c1 == 0:
            return result

        # 1. 漲破 7% 沒鎖漲停
        high_pct = (h0 - c1) / c1
        close_pct = (c0 - c1) / c1
        if high_pct >= 0.07 and close_pct < 0.095:
            result['漲破7沒鎖漲停'] = True

        # 2. 低開 3% 站不回紅
        open_pct = (o0 - c1) / c1
        if open_pct <= -0.03 and close_pct <= 0:
            result['低開3站不回紅'] = True

        # 3. 連漲 3 天
        c2 = pd.to_numeric(df_p['Close'].iloc[-3], errors='coerce')
        c3 = pd.to_numeric(df_p['Close'].iloc[-4], errors='coerce')
        if pd.notna(c2) and pd.notna(c3):
            if c0 > c1 and c1 > c2 and c2 > c3:
                result['連漲3天'] = True

        # 4. 高檔爆量
        if len(df_p) >= 20:
            vol_ma20 = pd.to_numeric(df_p['Volume'].tail(20), errors='coerce').mean()
            high_60 = pd.to_numeric(df_p['High'].tail(60), errors='coerce').max() if len(df_p) >= 60 else pd.to_numeric(df_p['High'], errors='coerce').max()
            is_high_price = (c0 >= high_60 * 0.90) if pd.notna(high_60) and high_60 > 0 else False
            is_high_vol = (v0 >= vol_ma20 * 2) if pd.notna(vol_ma20) and vol_ma20 > 0 else False
            if is_high_price and is_high_vol:
                result['高檔爆量'] = True

        # 5. 換手率
        shares_out = info.get('sharesOutstanding')
        if shares_out and shares_out > 0:
            turnover = (v0 / shares_out) * 100
            result['換手率'] = turnover
            if turnover < 5.0:
                result['換手率小於5'] = True

        suggestions = []
        if result['換手率小於5']:
            suggestions.append('跳過(換手<5%)')
        if result['漲破7沒鎖漲停']:
            suggestions.append('走(曾漲7%沒鎖)')
        if result['低開3站不回紅']:
            suggestions.append('走(低開不回紅)')
        if result['高檔爆量']:
            suggestions.append('走(高檔爆量)')
        if result['連漲3天']:
            suggestions.append('賣一半(連漲3日)')

        if suggestions:
            result['五大守則建議'] = ' | '.join(suggestions)
    except Exception:
        pass
    return result

# -------------------------------------------------------------
# 4. 主力籌碼與價量時機評分
# -------------------------------------------------------------

def evaluate_smart_money_signals(df_p, tdcc_row=None):
    """結合 TDCC 主力籌碼與均線糾結共振"""
    if df_p is None or len(df_p) < 40:
        return {
            '現價': np.nan, '主力評分': 50, '主力特徵': '資料不足',
            '時機建議': '觀望等待', '量能倍數': 1.0, '均線糾結度(%)': 0.0,
            '20日乖離率(%)': 0.0, '千張大戶比例(%)': 0.0, '400張大戶比例(%)': 0.0,
            '100張以下散戶比例(%)': 0.0, 'KD_K': 50.0, 'KD_D': 50.0
        }

    curr_p = float(df_p['Close'].iloc[-1])
    ma5 = float(df_p['MA5'].iloc[-1])
    ma20 = float(df_p['MA20'].iloc[-1])
    ma60 = float(df_p['MA60'].iloc[-1]) if 'MA60' in df_p.columns else ma20
    curr_vol = float(df_p['Volume'].iloc[-1])
    vol_ma20 = float(df_p['Vol_MA20'].iloc[-1]) if 'Vol_MA20' in df_p.columns else curr_vol
    kd_k = float(df_p['KD_K'].iloc[-1]) if 'KD_K' in df_p.columns else 50.0
    kd_d = float(df_p['KD_D'].iloc[-1]) if 'KD_D' in df_p.columns else 50.0
    bias_20 = float(df_p['Bias_20'].iloc[-1]) if 'Bias_20' in df_p.columns else 0.0

    ma_min = min(ma5, ma20, ma60)
    ma_band = ((max(ma5, ma20, ma60) - ma_min) / ma_min * 100) if ma_min > 0 else 10.0
    is_ma_entangled = ma_band < 4.5

    vol_ratio = (curr_vol / vol_ma20) if vol_ma20 > 0 else 1.0
    is_volume_breakout = vol_ratio >= 1.6 and curr_p > ma20 and curr_p >= df_p['Close'].iloc[-2]
    is_volume_dryup = vol_ratio <= 0.65 and abs(bias_20) < 3.0

    pct_over_1000 = tdcc_row['千張大戶比例(%)'] if (tdcc_row is not None and not tdcc_row.empty) else 0.0
    pct_over_400 = tdcc_row['400張大戶比例(%)'] if (tdcc_row is not None and not tdcc_row.empty) else 0.0
    pct_under_100 = tdcc_row['100張以下散戶比例(%)'] if (tdcc_row is not None and not tdcc_row.empty) else 0.0

    smart_score = 50
    if pct_over_1000 >= 60:
        smart_score += 20
    elif pct_over_1000 >= 45:
        smart_score += 12
    elif pct_over_1000 < 30:
        smart_score -= 10

    if pct_over_400 >= 70:
        smart_score += 10
    elif pct_over_400 >= 55:
        smart_score += 5

    if pct_under_100 < 25:
        smart_score += 10
    elif pct_under_100 > 45:
        smart_score -= 10

    if is_volume_breakout:
        smart_score += 15
    elif is_volume_dryup and is_ma_entangled:
        smart_score += 10

    if curr_p > ma20 and ma20 >= ma60:
        smart_score += 5
    if kd_k < 35 and kd_k > kd_d:
        smart_score += 5

    smart_score = min(100, max(0, smart_score))

    smart_signals = []
    if is_volume_breakout and (pct_over_1000 >= 45 or pct_over_400 >= 60):
        smart_signals.append("主力帶量突破")
    elif is_volume_dryup and is_ma_entangled:
        smart_signals.append("籌碼沉澱窒息量")

    if pct_over_1000 >= 60:
        smart_signals.append("千張大戶重倉(>60%)")
    if pct_under_100 < 25:
        smart_signals.append("散戶離場集中")

    smart_summary = "、".join(smart_signals) if smart_signals else "籌碼常態"

    decision = "觀望等待"
    if smart_score >= 75 and curr_p > ma20 and kd_k > kd_d and bias_20 < 8:
        decision = "★主力強勢買進"
    elif smart_score >= 65 and (is_volume_dryup or (curr_p >= ma20 and bias_20 < 4)):
        decision = "逢低分批佈局"
    elif bias_20 > 15 or (curr_p < ma20 and curr_p < ma60 and vol_ratio > 1.8):
        decision = "高檔停利/破線減碼"

    return {
        '現價': round(curr_p, 2),
        '主力評分': smart_score,
        '主力特徵': smart_summary,
        '時機建議': decision,
        '量能倍數': round(vol_ratio, 2),
        '均線糾結度(%)': round(ma_band, 2),
        '20日乖離率(%)': round(bias_20, 2),
        '千張大戶比例(%)': pct_over_1000,
        '400張大戶比例(%)': pct_over_400,
        '100張以下散戶比例(%)': pct_under_100,
        'KD_K': round(kd_k, 1),
        'KD_D': round(kd_d, 1)
    }

# -------------------------------------------------------------
# 5. 技術時機點判定 (從 Backtest.py 整合)
# -------------------------------------------------------------

def evaluate_timing_signals(df_p, shares_out=None):
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
    res['漲跌幅(%)'] = round(((c0 - c1) / c1) * 100, 2) if c1 > 0 else 0.0
    res['20MA'] = round(ma20, 2)
    res['20日均量'] = vol_ma20
    vol_ratio = (v0 / vol_ma20) if vol_ma20 > 0 else 1.0
    res['量能倍數'] = round(vol_ratio, 2)

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

    high_20 = highs.iloc[-21:-1].max() if len(highs) >= 21 else highs.max()
    high_60 = highs.iloc[-61:-1].max() if len(highs) >= 61 else high_20

    is_ma20_up = ma20 >= ma20_prev
    is_above_ma20 = c0 >= ma20
    cross_above_ma20 = (c1 < ma20_prev) and (c0 >= ma20)
    is_breakout_20 = (c0 > high_20) and (c0 > o0) and (vol_ratio >= 1.25)

    cross_below_ma20 = (c1 >= ma20_prev) and (c0 < ma20)
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
# 6. 策略回測引擎 (3 年波段 20MA + 主力籌碼動態回測)
# -------------------------------------------------------------

def run_backtest_for_symbol(df_p, stop_loss_pct=0.08, take_profit_pct=0.35):
    """歷史 3 年波段回測 (20MA 突破與出場機制)"""
    res_summary = {
        '總交易次數': 0, '獲利次數': 0, '勝率(%)': 0.0,
        '累積報酬率(%)': 0.0, '年化報酬率(%)': 0.0,
        '買進持有報酬(%)': 0.0, '盈虧比': 0.0, '最大回撤(MDD %)': 0.0,
        '當前持股狀態': '空手觀望', '當前持股損益(%)': 0.0,
        '持股進場日': '-', '持股進場價': 0.0
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
            is_breakout = (c0 > h20) and (v0 >= vol_ma20 * 1.2) if pd.notna(h20) else False
            cross_up = (c_prev < ma20_prev) and (c0 > ma20) and (ma20 >= ma20_prev) and (v0 >= vol_ma20 * 1.1)
            if is_breakout or cross_up:
                in_position = True
                entry_price = c0
                entry_date = curr_date
        else:
            gain_pct = (c0 - entry_price) / entry_price
            is_stop_loss = gain_pct <= -stop_loss_pct
            c_prev2 = df['Close'].iloc[i - 1]
            ma20_prev2 = df['MA20'].iloc[i - 1]
            drop_below_ma20 = (c0 < ma20 * 0.975) or ((c0 < ma20) and (c_prev2 < ma20_prev2))
            is_take_profit = (gain_pct >= take_profit_pct) and (c0 < ma5)

            if is_stop_loss or drop_below_ma20 or is_take_profit or (i == len(df) - 1):
                ret = (c0 - entry_price) / entry_price
                reason = '停損' if is_stop_loss else ('目標停利' if is_take_profit else '均線出場')

                if i == len(df) - 1 and not (is_stop_loss or drop_below_ma20 or is_take_profit):
                    res_summary['當前持股狀態'] = '持有中'
                    res_summary['當前持股損益(%)'] = round(gain_pct * 100, 2)
                    res_summary['持股進場日'] = entry_date.strftime('%Y-%m-%d') if hasattr(entry_date, 'strftime') else str(entry_date)[:10]
                    res_summary['持股進場價'] = round(entry_price, 2)
                else:
                    trades.append({'entry_date': entry_date, 'exit_date': curr_date, 'return': ret, 'reason': reason})
                    equity_curve.append(equity_curve[-1] * (1.0 + ret))
                    in_position = False

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

        trading_years = max((len(df) - 25) / 250, 0.5)
        cagr = ((1.0 + cum_ret) ** (1.0 / trading_years) - 1.0) if (1.0 + cum_ret) > 0 else -1.0
        res_summary['年化報酬率(%)'] = round(cagr * 100, 2)

        total_gain = sum(wins)
        total_loss = sum(losses)
        res_summary['盈虧比'] = round(total_gain / total_loss, 2) if total_loss > 0 else (99.0 if total_gain > 0 else 0.0)

        eq_series = pd.Series(equity_curve)
        rolling_max = eq_series.cummax()
        drawdown = (eq_series - rolling_max) / rolling_max
        res_summary['最大回撤(MDD %)'] = round(abs(drawdown.min()) * 100, 2)

    return res_summary

def run_smart_money_backtest(df_p):
    """主力籌碼時機點動態回測"""
    if df_p is None or len(df_p) < 60:
        return {'總報酬率(%)': 0.0, '交易次數': 0, '勝率(%)': 0.0, '最大回撤MDD(%)': 0.0}

    capital = 1000000
    pos = 0
    buy_price = 0
    trade_count = 0
    win_count = 0
    equity = [capital]

    for i in range(25, len(df_p)):
        sub = df_p.iloc[:i+1]
        c = float(sub['Close'].iloc[-1])
        ma20 = float(sub['MA20'].iloc[-1])
        ma60 = float(sub['MA60'].iloc[-1]) if 'MA60' in sub.columns else ma20
        v = float(sub['Volume'].iloc[-1])
        v_ma20 = float(sub['Vol_MA20'].iloc[-1]) if 'Vol_MA20' in sub.columns else v
        k = float(sub['KD_K'].iloc[-1]) if 'KD_K' in sub.columns else 50.0
        d = float(sub['KD_D'].iloc[-1]) if 'KD_D' in sub.columns else 50.0
        prev_k = float(sub['KD_K'].iloc[-2]) if 'KD_K' in sub.columns else 50.0
        prev_d = float(sub['KD_D'].iloc[-2]) if 'KD_D' in sub.columns else 50.0

        if pos == 0:
            vol_expansion = (v > v_ma20 * 1.5) if v_ma20 > 0 else False
            price_above_ma = c > ma20 and ma20 >= ma60
            kd_golden = (prev_k <= prev_d) and (k > d) and (k < 65)
            if price_above_ma and (vol_expansion or kd_golden):
                pos = capital / c
                buy_price = c
        elif pos > 0:
            ret = (c - buy_price) / buy_price
            bias = (c - ma20) / ma20 * 100
            stop_loss = (c < ma20 * 0.965) or (ret <= -0.07)
            take_profit = (ret >= 0.18) or (bias >= 15.0)
            if stop_loss or take_profit or (i == len(df_p) - 1):
                capital = pos * c
                pos = 0
                trade_count += 1
                if ret > 0:
                    win_count += 1

        current_val = (pos * c) if pos > 0 else capital
        equity.append(current_val)

    final_cap = (pos * float(df_p['Close'].iloc[-1])) if pos > 0 else capital
    tot_return = (final_cap - 1000000) / 1000000 * 100
    win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0

    eq_s = pd.Series(equity)
    cummax = eq_s.cummax()
    dd = (cummax - eq_s) / cummax * 100
    mdd = dd.max() if not dd.empty else 0.0

    return {
        '總報酬率(%)': round(tot_return, 2),
        '交易次數': trade_count,
        '勝率(%)': round(win_rate, 2),
        '最大回撤MDD(%)': round(mdd, 2)
    }

# -------------------------------------------------------------
# 7. 全方位整合執行管線 (Unified Pipeline)
# -------------------------------------------------------------

def main():
    start_time = datetime.datetime.now()
    print("=" * 75)
    print(" 🚀 啟動『Stock_GVI 全方位估值 × TDCC 主力籌碼 × 策略回測 整合引擎』")
    print("=" * 75)

    # 1. 取得股票代號清單
    stock_ids = get_stock_list()
    if not stock_ids:
        print("[錯誤] 未能取得標的清單，程式終止。")
        return

    ref_df = load_reference_data()

    # 2. 一次性下載 TDCC 集保籌碼
    tdcc_df = fetch_tdcc_shareholder_distribution()

    # 3. 整理 yfinance 查詢清單
    symbols = []
    symbol_to_id = {}
    for s in stock_ids:
        sym = s if ('.TW' in s or '.TWO' in s) else f"{s}.TW"
        symbols.append(sym)
        symbol_to_id[sym] = sym.split('.')[0]

    # 4. 一次性批量下載 3 年價量數據
    print(f"\n[yfinance] 正在一次性批量下載 {len(symbols)} 檔標的近 3 年歷史日 K 線...")
    try:
        history_data = yf.download(
            symbols,
            period="3y",
            group_by='ticker',
            threads=True,
            auto_adjust=True,
            progress=False
        )
    except Exception as e:
        print(f"[錯誤] 批次下載歷史價量失敗: {e}")
        history_data = pd.DataFrame()

    # 5. 逐檔進行多維度深度分析
    print("\n[分析] 開始計算基本面、籌碼集中度、技術時機點與波段歷史回測...")
    raw_results = []
    backtest_perf_rows = []
    smart_money_bt_rows = []

    for idx, sym in enumerate(symbols, 1):
        sid = symbol_to_id[sym]
        name = get_tw_name(sym)
        ref_data = ref_df.loc[sid] if (not ref_df.empty and sid in ref_df.index) else None
        tdcc_row = tdcc_df.loc[sid] if (not tdcc_df.empty and sid in tdcc_df.index) else None

        # 取得個股價量
        df_p = pd.DataFrame()
        try:
            if isinstance(history_data.columns, pd.MultiIndex):
                if sym in history_data.columns.levels[0]:
                    df_p = history_data[sym].dropna(subset=['Close']).copy()
            else:
                df_p = history_data.dropna(subset=['Close']).copy()
        except Exception:
            pass

        # 計算移動均線與指標
        if not df_p.empty and len(df_p) >= 20:
            df_p['MA5'] = df_p['Close'].rolling(5).mean()
            df_p['MA20'] = df_p['Close'].rolling(20).mean()
            df_p['MA60'] = df_p['Close'].rolling(60).mean() if len(df_p) >= 60 else df_p['MA20']
            df_p['Vol_MA5'] = df_p['Volume'].rolling(5).mean()
            df_p['Vol_MA20'] = df_p['Volume'].rolling(20).mean()
            df_p['Bias_20'] = (df_p['Close'] - df_p['MA20']) / df_p['MA20'] * 100

            low_9 = df_p['Low'].rolling(9).min()
            high_9 = df_p['High'].rolling(9).max()
            rsv = (df_p['Close'] - low_9) / (high_9 - low_9 + 1e-9) * 100
            df_p['KD_K'] = rsv.ewm(com=2).mean()
            df_p['KD_D'] = df_p['KD_K'].ewm(com=2).mean()

        # 個股基本面物件讀取
        ticker = yf.Ticker(sym)
        try:
            info = ticker.info or {}
        except Exception:
            info = {}

        # 現價與收盤日期
        close = np.nan
        close = info.get('currentPrice') or info.get('previousClose')
        if (pd.isna(close) or close == 0) and not df_p.empty:
            close = df_p['Close'].iloc[-1]
        if (pd.isna(close) or close == 0) and ref_data is not None:
            close = parse_float(ref_data.get('收盤'))

        close_date = ""
        if not df_p.empty:
            close_date = df_p.index[-1].strftime('%Y-%m-%d') if hasattr(df_p.index[-1], 'strftime') else str(df_p.index[-1])[:10]

        # 估值指標 (PE, PB, ROE)
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

        # GVI 計算: (1/PB) * (1 + ROE)^5
        roe_ratio = roe / 100 if (roe and roe > 1) else roe
        gvi = np.nan
        if pb and pb > 0 and roe_ratio and roe_ratio > 0:
            gvi = (1 / pb) * ((1 + roe_ratio) ** 5)
        elif ref_data is not None:
            gvi = parse_float(ref_data.get('GVI指標'))

        # 殖利率
        div_yield = np.nan
        if ref_data is not None:
            raw_yield = parse_float(ref_data.get('殖利率'))
            if not pd.isna(raw_yield):
                div_yield = raw_yield / 100
        if pd.isna(div_yield):
            div_rate = info.get('dividendRate', 0)
            div_yield = (div_rate / close * 100) if (div_rate and close) else 0

        # 獲利指標
        gross_margin = np.nan
        op_margin = np.nan
        net_margin = np.nan
        pretax_margin = np.nan
        if ref_data is not None:
            gross_margin = parse_float(ref_data.get('毛利率'))
            op_margin = parse_float(ref_data.get('營業利益率'))
            net_margin = parse_float(ref_data.get('稅後淨利率'))

        if pd.isna(gross_margin):
            gross_margin = info.get('grossMargins', 0) * 100
        if pd.isna(op_margin):
            op_margin = info.get('operatingMargins', 0) * 100
        if pd.isna(net_margin):
            net_margin = info.get('profitMargins', 0) * 100

        revenue_growth = info.get('revenueGrowth', 0) * 100
        earnings_growth = info.get('earningsGrowth', 0) * 100

        try:
            fin = ticker.financials
            if not fin.empty and 'Pretax Income' in fin.index and 'Total Revenue' in fin.index:
                pretax_income = fin.loc['Pretax Income'].iloc[0]
                tot_rev = fin.loc['Total Revenue'].iloc[0]
                if tot_rev != 0:
                    pretax_margin = (pretax_income / tot_rev) * 100
        except Exception:
            pass

        core_ratio = np.nan
        denom = pretax_margin if (pretax_margin and not pd.isna(pretax_margin)) else net_margin
        if not pd.isna(op_margin) and not pd.isna(denom) and denom > 0:
            core_ratio = (op_margin / denom * 100)

        # 5年最低 PE 與 PE 區間
        pe_min_5y = calculate_min_pe_5y(ticker, history_data, sym)
        pe_range_str = "-"
        if ref_data is not None and '本益比區間' in ref_data:
            pe_range_str = str(ref_data['本益比區間'])
        if pe_range_str in ["-", "nan"]:
            eps = info.get('trailingEps')
            if not df_p.empty and eps and eps > 0 and close:
                high_y = df_p['Close'].tail(250).max()
                low_y = df_p['Close'].tail(250).min()
                pe_range_str = f"{close / eps:.1f} [{low_y / eps:.1f}-{high_y / eps:.1f}]"

        # 財務健康指標 (資產負債與現金流)
        free_cash_flow = np.nan
        operating_cash_flow = np.nan
        debt_ratio = np.nan
        current_ratio = np.nan
        quick_ratio = np.nan
        cash_flow_ratio = np.nan
        sector = info.get('sector', '-')
        industry = info.get('industry', '-')

        try:
            cf = ticker.cashflow
            if not cf.empty:
                if 'Free Cash Flow' in cf.index:
                    free_cash_flow = cf.loc['Free Cash Flow'].iloc[0]
                if 'Operating Cash Flow' in cf.index:
                    operating_cash_flow = cf.loc['Operating Cash Flow'].iloc[0]

            bs = ticker.balance_sheet
            if not bs.empty:
                total_assets = bs.loc['Total Assets'].iloc[0] if 'Total Assets' in bs.index else np.nan
                total_liab = bs.loc['Total Liabilities Net Minority Interest'].iloc[0] if 'Total Liabilities Net Minority Interest' in bs.index else np.nan
                current_assets = bs.loc['Current Assets'].iloc[0] if 'Current Assets' in bs.index else np.nan
                current_liab = bs.loc['Current Liabilities'].iloc[0] if 'Current Liabilities' in bs.index else np.nan
                inventory = bs.loc['Inventory'].iloc[0] if 'Inventory' in bs.index else 0

                if not pd.isna(total_liab) and not pd.isna(total_assets) and total_assets > 0:
                    debt_ratio = (total_liab / total_assets) * 100
                if not pd.isna(current_assets) and not pd.isna(current_liab) and current_liab > 0:
                    current_ratio = (current_assets / current_liab) * 100
                    quick_ratio = ((current_assets - inventory) / current_liab) * 100

            income_stmt = ticker.financials
            if not income_stmt.empty and not pd.isna(operating_cash_flow):
                net_inc = income_stmt.loc['Net Income'].iloc[0] if 'Net Income' in income_stmt.index else np.nan
                if not pd.isna(net_inc) and net_inc > 0:
                    cash_flow_ratio = (operating_cash_flow / net_inc) * 100
        except Exception:
            pass

        if pd.isna(debt_ratio):
            dte = info.get('debtToEquity', np.nan)
            if not pd.isna(dte):
                debt_ratio = (dte / (100 + dte)) * 100
        if pd.isna(current_ratio):
            cr = info.get('currentRatio', np.nan)
            if not pd.isna(cr):
                current_ratio = cr * 100
        if pd.isna(free_cash_flow):
            free_cash_flow = info.get('freeCashflow', np.nan)

        # 體質評等
        is_financial = 'Financial' in str(sector) or '金控' in str(info.get('longName', ''))
        unqualified_reasons = []
        min_roe = 8.0 if is_financial else 10.0
        if pd.isna(roe) or roe < min_roe:
            unqualified_reasons.append(f'ROE<{min_roe}%')
        if not is_financial and pd.notna(op_margin) and op_margin <= 0:
            unqualified_reasons.append('本業虧損')
        if not is_financial and pd.notna(debt_ratio) and debt_ratio > 65:
            unqualified_reasons.append('負債比>65%')

        if len(unqualified_reasons) == 0:
            quality_grade = '優良'
        elif len(unqualified_reasons) == 1:
            quality_grade = '尚可'
        else:
            quality_grade = '偏弱'

        # 成交量與五大守則
        vol_analysis = calculate_volume_analysis(df_p)
        rules_res = check_five_trading_rules(df_p, info)

        # TDCC 主力籌碼評估
        shares_out = info.get('sharesOutstanding', None)
        smart_res = evaluate_smart_money_signals(df_p, tdcc_row)

        # 技術時機評估
        timing_res = evaluate_timing_signals(df_p, shares_out)

        # 3 年波段策略回測
        bt_res = run_backtest_for_symbol(df_p)

        # 1 年主力籌碼策略回測
        smart_bt_res = run_smart_money_backtest(df_p)

        # 20 日報酬率
        r_20d = np.nan
        if not df_p.empty and len(df_p) >= 20:
            r_20d = (df_p['Close'].iloc[-1] - df_p['Close'].iloc[-20]) / df_p['Close'].iloc[-20]

        # 記錄回測表現列
        backtest_perf_rows.append({
            '證券代號': sym,
            '證券名稱': name,
            '累積報酬率': f"{bt_res['累積報酬率(%)']:.1f}%",
            '年化報酬率(CAGR)': f"{bt_res['年化報酬率(%)']:.1f}%",
            '勝率': f"{bt_res['勝率(%)']:.1f}%",
            '盈虧比': f"{bt_res['盈虧比']:.2f}",
            '最大回撤(MDD)': f"{bt_res['最大回撤(MDD %)']:.1f}%",
            '總交易次數': bt_res['總交易次數'],
            '買進持有報酬': f"{bt_res['買進持有報酬(%)']:.1f}%",
            '當前狀態': bt_res['當前持股狀態'],
            '當前損益': f"{bt_res['當前持股損益(%)']:.1f}%" if bt_res['當前持股狀態'] == '持有中' else '-',
            '進場日期': bt_res['持股進場日']
        })

        smart_money_bt_rows.append({
            '證券代號': sid,
            '名稱': name,
            '主力評分': smart_res['主力評分'],
            '時機建議': smart_res['時機建議'],
            '千張大戶比例(%)': smart_res['千張大戶比例(%)'],
            '總報酬率(%)': smart_bt_res['總報酬率(%)'],
            '勝率(%)': smart_bt_res['勝率(%)'],
            '交易次數': smart_bt_res['交易次數'],
            '最大回撤MDD(%)': smart_bt_res['最大回撤MDD(%)']
        })

        # 收集原始綜合資料列
        raw_results.append({
            '證券代號': sym,
            '證券代號_純碼': sid,
            '證券名稱': name,
            '收盤': close,
            '收盤日期': close_date,
            '漲跌幅': timing_res.get('漲跌幅(%)', 0.0),
            'GVI指標': gvi,
            '20日報酬率': r_20d,
            '殖利率': div_yield,
            '本益比': pe,
            '淨值比': pb,
            'ROE': roe,
            '外資持股(%)': info.get('heldPercentInstitutions', 0) * 100 if info.get('heldPercentInstitutions') else np.nan,
            '張數': int(info.get('volume', 0) / 1000) if info.get('volume') else 0,
            '毛利率': gross_margin,
            '營業利益率': op_margin,
            '稅後淨利率': net_margin,
            '稅前淨利率': pretax_margin,
            '本業比例': core_ratio,
            '營收成長率': revenue_growth,
            '淨利成長率': earnings_growth,
            '近五年最低本益比': pe_min_5y,
            '本益比區間': pe_range_str,
            '自由現金流': free_cash_flow,
            '負債比率': debt_ratio,
            '流動比率': current_ratio,
            '速動比率': quick_ratio,
            '現金流量比': cash_flow_ratio,
            '產業': sector,
            '細產業': industry,
            '5日均量': vol_analysis['vol_ma5'],
            '20日均量': vol_analysis['vol_ma20'],
            '量能倍數': vol_analysis['vol_ratio'],
            '量能訊號': vol_analysis['vol_signal'],
            '五大守則建議': rules_res.get('五大守則建議', '-'),
            '換手率': rules_res.get('換手率', np.nan),
            # 主力籌碼欄位
            '主力評分': smart_res['主力評分'],
            '主力籌碼特徵': smart_res['主力特徵'],
            '時機建議': smart_res['時機建議'],
            '千張大戶比例(%)': smart_res['千張大戶比例(%)'],
            '400張大戶比例(%)': smart_res['400張大戶比例(%)'],
            '100張以下散戶比例(%)': smart_res['100張以下散戶比例(%)'],
            '均線糾結度(%)': smart_res['均線糾結度(%)'],
            '20日乖離率(%)': smart_res['20日乖離率(%)'],
            'KD_K': smart_res['KD_K'],
            'KD_D': smart_res['KD_D'],
            # 回測與技術欄位
            '20MA': timing_res.get('20MA', np.nan),
            '技術時機點': timing_res.get('技術時機訊號', '-'),
            '體質評等': quality_grade,
            '體質未達標原因': "; ".join(unqualified_reasons) if unqualified_reasons else "無",
            '流動性狀態': timing_res.get('流動性狀態', '充足'),
            '回測_年化報酬(%)': bt_res['年化報酬率(%)'],
            '回測_勝率(%)': bt_res['勝率(%)'],
            '回測_盈虧比': bt_res['盈虧比'],
            '回測_MDD(%)': bt_res['最大回撤(MDD %)'],
            '回測_累積報酬(%)': bt_res['累積報酬率(%)'],
            '回測持股狀態': bt_res['當前持股狀態'],
        })

        if idx % 15 == 0 or idx == len(symbols):
            print(f"  > 進度: [{idx}/{len(symbols)}] 已完成 {name} ({sym})")

    df_all = pd.DataFrame(raw_results)

    # 6. 計算三因子、三好一公道、四道關卡、財務健康等評分
    print("\n[計分] 計算綜合排名、四道關卡、三好一公道與品質篩選...")
    # 百分位排名三因子
    df_all['淨值比'] = pd.to_numeric(df_all['淨值比'], errors='coerce')
    df_all['ROE'] = pd.to_numeric(df_all['ROE'], errors='coerce')
    df_all['20日報酬率'] = pd.to_numeric(df_all['20日報酬率'], errors='coerce')

    valid_mask = (df_all['淨值比'] > 0) & (df_all['ROE'] > 0)
    df_all['ROE_Score'] = np.nan
    df_all['PB_Score'] = np.nan
    df_all['R_Score'] = np.nan
    df_all['三因子評分'] = np.nan

    if valid_mask.any():
        df_all.loc[valid_mask, 'ROE_Score'] = df_all.loc[valid_mask, 'ROE'].rank(pct=True) * 100
        df_all.loc[valid_mask, 'PB_Score'] = (1 / df_all.loc[valid_mask, '淨值比']).rank(pct=True) * 100
        df_all.loc[valid_mask, 'R_Score'] = df_all.loc[valid_mask, '20日報酬率'].rank(pct=True) * 100
        df_all.loc[valid_mask, '三因子評分'] = (
            df_all.loc[valid_mask, 'ROE_Score'] * 0.33 +
            df_all.loc[valid_mask, 'PB_Score'] * 0.33 +
            df_all.loc[valid_mask, 'R_Score'] * 0.34
        )

    # 財務健康評分
    df_all['財務健康評分'] = 0.0
    df_all.loc[df_all['自由現金流'] > 0, '財務健康評分'] += 20
    df_all.loc[df_all['負債比率'] < 50, '財務健康評分'] += 20
    df_all.loc[df_all['流動比率'] > 150, '財務健康評分'] += 15
    df_all.loc[df_all['本業比例'] > 60, '財務健康評分'] += 15
    df_all.loc[df_all['現金流量比'] > 80, '財務健康評分'] += 15
    pe_reasonable = (df_all['本益比'] < 15) & (df_all['本益比'] > 0)
    pe_undervalued = (df_all['本益比'] < df_all['近五年最低本益比']) & (df_all['本益比'] > 0)
    df_all.loc[pe_reasonable | pe_undervalued, '財務健康評分'] += 15

    # 四道關卡
    c1 = (df_all['ROE'] > 15) & (df_all['毛利率'] > 20) & (df_all['營業利益率'] > 10)
    c2 = (df_all['現金流量比'] > 80) & (df_all['自由現金流'] > 0) & (df_all['負債比率'] < 50)
    c3 = (df_all['營收成長率'] > 0) & (df_all['淨利成長率'] > 0)
    df_all['L1_獲利能力'] = c1.map({True: 'Pass', False: 'Fail'})
    df_all['L2_財務安全'] = c2.map({True: 'Pass', False: 'Fail'})
    df_all['L3_成長動能'] = c3.map({True: 'Pass', False: 'Fail'})
    df_all['四關卡_基本面'] = (c1 & c2 & c3).map({True: '優良', False: '未達標'})

    def get_valuation_status(row):
        pe_v = row.get('本益比', np.nan)
        min_pe = row.get('近五年最低本益比', np.nan)
        if pd.isna(pe_v) or pe_v <= 0:
            return 'N/A'
        if (pe_v < 12) or (not pd.isna(min_pe) and min_pe > 0 and pe_v < min_pe * 1.1):
            return '便宜'
        if pe_v > 25:
            return '昂貴'
        return '合理'

    df_all['估值狀態'] = df_all.apply(get_valuation_status, axis=1)

    def get_decision(row):
        fund_pass = (row['L1_獲利能力'] == 'Pass') and (row['L2_財務安全'] == 'Pass') and (row['L3_成長動能'] == 'Pass')
        val = row['估值狀態']
        if fund_pass:
            if val == '便宜':
                return '★強烈買進'
            elif val == '合理':
                return '持有/買進'
            elif val == '昂貴':
                return '分批獲利'
            else:
                return '觀察'
        else:
            if val == '昂貴':
                return '★賣出 (貴且差)'
            else:
                return '基本面未標'

    df_all['決策建議'] = df_all.apply(get_decision, axis=1)

    # 三好一公道
    roe_good = df_all['ROE'] >= 12
    net_debt_good = df_all['負債比率'] <= 50
    eps_growth_good = df_all['淨利成長率'] > 0
    pe_fair = (df_all['本益比'] > 0) & (df_all['本益比'] <= 20)
    df_all['三好_ROE'] = roe_good.map({True: 'Pass', False: 'Fail'})
    df_all['三好_淨負債'] = net_debt_good.map({True: 'Pass', False: 'Fail'})
    df_all['三好_EPS成長'] = eps_growth_good.map({True: 'Pass', False: 'Fail'})
    df_all['一公道_本益比'] = pe_fair.map({True: 'Pass', False: 'Fail'})
    df_all['三好一公道分數'] = (roe_good.astype(int) + net_debt_good.astype(int) + eps_growth_good.astype(int) + pe_fair.astype(int)) * 25

    def grade_three_good(s):
        if s >= 100:
            return '★三好一公道'
        if s >= 75:
            return '三好一公道(佳)'
        if s >= 50:
            return '觀察'
        return '待加強'

    df_all['三好一公道評等'] = df_all['三好一公道分數'].apply(grade_three_good)

    # CS選股條件評估
    a1 = (df_all['本益比'] > 0) & (df_all['本益比'] < 15)
    div_p = df_all['殖利率'] * 100 if df_all['殖利率'].max(skipna=True) < 1 else df_all['殖利率']
    a2 = div_p > 5
    b1 = (df_all['本益比'] > 0) & (df_all['本益比'] < 10)
    b2 = (df_all['本益比'] > 0) & (df_all['本益比'] < df_all.get('近五年最低本益比', 0))
    c1_cs = df_all['營收成長率'] > 0
    c2_cs = df_all['毛利率'] > 0
    c3_cs = df_all['營業利益率'] > 0
    c6_cs = df_all['本業比例'] > 60
    c7_cs = df_all['ROE'] > 10

    df_all['CS_A1_本益比<15'] = a1.map({True: 'Pass', False: 'Fail'})
    df_all['CS_A2_殖利率>5%'] = a2.map({True: 'Pass', False: 'Fail'})
    df_all['CS_B1_本益比<10'] = b1.map({True: 'Pass', False: 'Fail'})
    df_all['CS_B2_低於五年最低PE'] = b2.map({True: 'Pass', False: 'Fail'})
    df_all['CS_C1_營收成長>0'] = c1_cs.map({True: 'Pass', False: 'Fail'})
    df_all['CS_C2_毛利率>0'] = c2_cs.map({True: 'Pass', False: 'Fail'})
    df_all['CS_C3_營業利益率>0'] = c3_cs.map({True: 'Pass', False: 'Fail'})
    df_all['CS_C6_本業比例>60%'] = c6_cs.map({True: 'Pass', False: 'Fail'})
    df_all['CS_C7_ROE>10'] = c7_cs.map({True: 'Pass', False: 'Fail'})
    df_all['CS_總得分'] = (a1.astype(int) + a2.astype(int) + b1.astype(int) + b2.astype(int) +
                         c1_cs.astype(int) + c2_cs.astype(int) + c3_cs.astype(int) + c6_cs.astype(int) + c7_cs.astype(int))

    def cs_grade(s):
        if s >= 8:
            return '★極優'
        if s >= 6:
            return '優良'
        if s >= 4:
            return '中等'
        return '待加強'

    df_all['CS選股評等'] = df_all['CS_總得分'].apply(cs_grade)

    # 寬鬆品質篩選
    fail_pe = (df_all['本益比'] >= 20) | (df_all['本益比'] <= 0)
    fail_debt = df_all['負債比率'] >= 60
    fail_core = df_all['本業比例'] <= 50
    df_all['通過品質篩選'] = ~(fail_pe | fail_debt | fail_core)
    df_all['排除原因'] = ''
    df_all.loc[fail_pe, '排除原因'] += '高PE;'
    df_all.loc[fail_debt, '排除原因'] += '高負債;'
    df_all.loc[fail_core, '排除原因'] += '低本業;'

    # 7. 匯出檔案 (Export Datasets)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    public_dir = os.path.join(os.path.dirname(current_dir), 'public')
    os.makedirs(public_dir, exist_ok=True)

    print("\n[匯出] 正在匯出全方位整合資料集與相容報表...")

    # A. 🌟 全方位整合大表: Stock_GVI_Unified.csv
    # 依 主力評分 與 年化報酬率 排序
    df_unified = df_all[[
        '證券代號', '證券名稱', '收盤', '漲跌幅', '決策建議', '主力評分',
        '時機建議', '主力籌碼特徵', '千張大戶比例(%)', '400張大戶比例(%)',
        '100張以下散戶比例(%)', '技術時機點', '體質評等', 'GVI指標',
        '三好一公道評等', '本益比', '淨值比', 'ROE', '殖利率',
        '回測_年化報酬(%)', '回測_勝率(%)', '回測_盈虧比', '回測_MDD(%)',
        '回測持股狀態', '五大守則建議', '產業'
    ]].copy()

    df_unified = df_unified.rename(columns={
        '收盤': '收盤價',
        '漲跌幅': '漲跌幅(%)',
        'ROE': 'ROE(%)',
        '殖利率': '殖利率(%)',
        '回測_年化報酬(%)': '年化報酬率(CAGR)',
        '回測_勝率(%)': '勝率(%)',
        '回測_盈虧比': '盈虧比',
        '回測_MDD(%)': '最大回撤(MDD)'
    })

    df_unified = df_unified.sort_values(by=['主力評分', '年化報酬率(CAGR)'], ascending=[False, False])
    out_unified_path = os.path.join(public_dir, 'Stock_GVI_Unified.csv')
    df_unified.to_csv(out_unified_path, index=False, encoding='utf-8-sig')
    print(f"  [✓] 產出全方位整合大表: {out_unified_path}")

    # B. 📑 相容輸出 1: Stock_GVI_ThreeFactor.csv (73 欄詳細版)
    date_str = df_all['收盤日期'].mode()[0] if not df_all['收盤日期'].dropna().empty else ""
    close_col_name = f'收盤 ({date_str})' if date_str else '收盤'

    df_three_factor = df_all.copy()
    if date_str and '收盤' in df_three_factor.columns:
        df_three_factor = df_three_factor.rename(columns={'收盤': close_col_name})

    tf_cols_order = [
        '證券代號', '證券名稱', close_col_name, '決策建議', '五大守則建議', '換手率',
        '三好一公道評等', '三好一公道分數', '三好_ROE', '三好_淨負債', '三好_EPS成長', '一公道_本益比',
        '四關卡_基本面', '估值狀態', 'L1_獲利能力', 'L2_財務安全', 'L3_成長動能',
        'CS選股評等', 'CS_總得分', 'CS_A1_本益比<15', 'CS_A2_殖利率>5%', 'CS_B1_本益比<10', 'CS_B2_低於五年最低PE',
        'CS_C1_營收成長>0', 'CS_C2_毛利率>0', 'CS_C3_營業利益率>0', 'CS_C6_本業比例>60%', 'CS_C7_ROE>10',
        'GVI指標', '三因子評分', '財務健康評分', '通過品質篩選', '20日報酬率', '5日均量', '20日均量',
        '量能倍數', '量能訊號', '殖利率', '本益比', '近五年最低本益比', '淨值比', 'ROE', '外資持股(%)',
        '張數', '毛利率', '營業利益率', '稅後淨利率', '稅前淨利率', '本業比例', '營收成長率',
        '淨利成長率', '自由現金流', '負債比率', '流動比率', '速動比率', '現金流量比', '本益比區間',
        '產業', '細產業', '排除原因'
    ]
    # 保留存在的欄位
    available_tf_cols = [c for c in tf_cols_order if c in df_three_factor.columns]
    out_tf_path = os.path.join(public_dir, 'Stock_GVI_ThreeFactor.csv')
    df_three_factor[available_tf_cols].to_csv(out_tf_path, index=False, encoding='utf-8-sig')
    print(f"  [✓] 產出三因子原始詳細清單: {out_tf_path}")

    # C. ⚡ 相容輸出 2: Stock_SmartMoney_Signals.csv
    df_smart_signals = df_all[[
        '證券代號_純碼', '證券名稱', '收盤', '主力評分', '時機建議', '主力籌碼特徵',
        '千張大戶比例(%)', '400張大戶比例(%)', '100張以下散戶比例(%)', '量能倍數',
        '均線糾結度(%)', '20日乖離率(%)', 'KD_K', 'KD_D', 'GVI指標', '淨值比',
        'ROE', '殖利率'
    ]].copy()
    df_smart_signals = df_smart_signals.rename(columns={
        '證券代號_純碼': '證券代號', '證券名稱': '名稱', '收盤': '現價',
        'GVI指標': 'GVI', '淨值比': '股價淨值比PB', 'ROE': 'ROE(%)', '殖利率': '殖利率(%)'
    }).sort_values(by='主力評分', ascending=False)
    out_smart_sig_path = os.path.join(public_dir, 'Stock_SmartMoney_Signals.csv')
    df_smart_signals.to_csv(out_smart_sig_path, index=False, encoding='utf-8-sig')
    print(f"  [✓] 產出主力籌碼訊號表: {out_smart_sig_path}")

    # D. ⚡ 相容輸出 3: Stock_SmartMoney_Backtest.csv
    df_smart_bt = pd.DataFrame(smart_money_bt_rows).sort_values(by='總報酬率(%)', ascending=False)
    out_smart_bt_path = os.path.join(public_dir, 'Stock_SmartMoney_Backtest.csv')
    df_smart_bt.to_csv(out_smart_bt_path, index=False, encoding='utf-8-sig')
    print(f"  [✓] 產出主力籌碼回測表: {out_smart_bt_path}")

    # E. 📈 相容輸出 4: Stock_Backtest_Signals.csv
    df_bt_signals = df_all[[
        '證券代號', '證券名稱', '收盤', '漲跌幅', '決策建議', '技術時機點',
        '體質評等', 'GVI指標', '本益比', '淨值比', 'ROE', '殖利率',
        '20MA', '量能倍數', '換手率', '流動性狀態', '回測持股狀態', '產業'
    ]].copy().rename(columns={'收盤': '收盤價'})
    out_bt_sig_path = os.path.join(public_dir, 'Stock_Backtest_Signals.csv')
    df_bt_signals.to_csv(out_bt_sig_path, index=False, encoding='utf-8-sig')
    print(f"  [✓] 產出回測買賣時機表: {out_bt_sig_path}")

    # F. 📈 相容輸出 5: Stock_Backtest_Performance.csv
    df_bt_perf = pd.DataFrame(backtest_perf_rows)
    if not df_bt_perf.empty:
        def parse_cagr(x):
            try:
                return float(str(x).replace('%', ''))
            except:
                return -999.0
        df_bt_perf['cagr_sort'] = df_bt_perf['年化報酬率(CAGR)'].apply(parse_cagr)
        df_bt_perf = df_bt_perf.sort_values(by='cagr_sort', ascending=False).drop(columns=['cagr_sort'])
    out_bt_perf_path = os.path.join(public_dir, 'Stock_Backtest_Performance.csv')
    df_bt_perf.to_csv(out_bt_perf_path, index=False, encoding='utf-8-sig')
    print(f"  [✓] 產出波段回測排行榜: {out_bt_perf_path}")

    elapsed = datetime.datetime.now() - start_time
    print("=" * 75)
    print(f"🎉 全部運算與匯出完成！總耗時: {elapsed.total_seconds():.1f} 秒")
    print("=" * 75)

    print("\n【🌟 全方位綜合排行榜 Top 10】")
    preview_cols = ['證券代號', '證券名稱', '收盤價', '決策建議', '主力評分', '千張大戶比例(%)', '年化報酬率(CAGR)', '勝率(%)', '體質評等']
    print(df_unified[preview_cols].head(10).to_string(index=False))

if __name__ == '__main__':
    main()
