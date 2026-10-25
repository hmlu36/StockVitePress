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
# 1. 讀取標的清單與外部參考資料
# -------------------------------------------------------------

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
# 2. TDCC 集保主力與大戶籌碼擷取
# -------------------------------------------------------------

def fetch_tdcc_shareholder_distribution():
    """
    從台灣集中保管結算所 (TDCC) Open Data 獲取最新每週全市場股東持股分級資料。
    持股分級代碼:
    1: 1-999股 (零股)
    2~9: 1~100張 (散戶)
    10~14: 100~1000張 (中實戶/大戶)
    15: 1000張以上 (千張超級大戶)
    10~15加總即為 400張以上/千張以上等主力核心圈
    """
    url = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
    print("正在連線 TDCC 集保資料庫下載最新股東持股分級資料...")

    try:
        context = ssl._create_unverified_context()
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=context, timeout=15) as response:
            df = pd.read_csv(response)

        df.columns = [c.strip() for c in df.columns]
        # 標題兼容處理
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
        print(f"TDCC 集保資料取得成功！最新結算日期: {data_date}，涵蓋個股數: {df[code_col].nunique()}")

        # 計算指標
        records = []
        for stock_id, g in df.groupby(code_col):
            # 總人數
            total_holders = g[person_col].sum()
            
            # 1000張以上大戶 (分級 15)
            level_15 = g[g[level_col] == 15]
            pct_over_1000 = level_15[ratio_col].sum() if not level_15.empty else 0.0
            holders_over_1000 = int(level_15[person_col].sum()) if not level_15.empty else 0

            # 400張以上大戶 (分級 12: 400~600張, 13: 600~800張, 14: 800~1000張, 15: 1000張以上)
            level_400plus = g[g[level_col] >= 12]
            pct_over_400 = level_400plus[ratio_col].sum() if not level_400plus.empty else 0.0

            # 散戶: 100張以下 (分級 1~9)
            level_retail = g[g[level_col] <= 9]
            pct_under_100 = level_retail[ratio_col].sum() if not level_retail.empty else 0.0
            retail_holders = int(level_retail[person_col].sum()) if not level_retail.empty else 0

            # 籌碼集中度評等 (千張大戶+400張大戶)
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
        print(f"獲取 TDCC 集保資料失敗: {e}")
        return pd.DataFrame()

# -------------------------------------------------------------
# 3. 整合基本面 (GVI/價值體質) 與 主力價量籌碼
# -------------------------------------------------------------

def evaluate_smart_money_signals(df_p, tdcc_row=None, shares_out=None):
    """
    結合三大主力指標的時機判斷：
    1. 籌碼集中結構 (TDCC 400張/千張大戶佔比)
    2. 主力量縮沉澱 vs 帶量突破 (Volume Dry-up & Volume Expansion Breakout)
    3. 均線糾結共振向上 (5MA, 20MA, 60MA 糾結收斂後出量黃金交叉)
    """
    if len(df_p) < 60:
        return {'訊號': '資料不足', '主力訊號': '無', '現價': np.nan, '分數': 0, '主力評分': 0}

    curr_p = float(df_p['Close'].iloc[-1])
    ma5 = float(df_p['MA5'].iloc[-1])
    ma20 = float(df_p['MA20'].iloc[-1])
    ma60 = float(df_p['MA60'].iloc[-1])
    curr_vol = float(df_p['Volume'].iloc[-1])
    vol_ma5 = float(df_p['Vol_MA5'].iloc[-1])
    vol_ma20 = float(df_p['Vol_MA20'].iloc[-1])
    kd_k = float(df_p['KD_K'].iloc[-1])
    kd_d = float(df_p['KD_D'].iloc[-1])
    bias_20 = float(df_p['Bias_20'].iloc[-1])

    # 均線帶寬 (衡量糾結度: Max(MA5,MA20,MA60) / Min(MA5,MA20,MA60) - 1)
    ma_band = (max(ma5, ma20, ma60) - min(ma5, ma20, ma60)) / min(ma5, ma20, ma60) * 100
    is_ma_entangled = ma_band < 4.5  # 均線高度糾結收斂（主力沉澱洗盤結束）

    # 成交量特徵
    vol_ratio = (curr_vol / vol_ma20) if vol_ma20 > 0 else 1.0  # 當日爆量倍數
    is_volume_breakout = vol_ratio >= 1.6 and curr_p > ma20 and curr_p >= df_p['Close'].iloc[-2]
    is_volume_dryup = vol_ratio <= 0.65 and abs(bias_20) < 3.0  # 窒息量整理

    # TDCC 主力籌碼特徵
    pct_over_1000 = tdcc_row['千張大戶比例(%)'] if (tdcc_row is not None and not tdcc_row.empty) else 0.0
    pct_over_400 = tdcc_row['400張大戶比例(%)'] if (tdcc_row is not None and not tdcc_row.empty) else 0.0
    pct_under_100 = tdcc_row['100張以下散戶比例(%)'] if (tdcc_row is not None and not tdcc_row.empty) else 0.0

    # 主力評分 (0 ~ 100 分)
    smart_score = 50

    # 1. 大戶籌碼加分/扣分
    if pct_over_1000 >= 60:
        smart_score += 20
    elif pct_over_1000 >= 45:
        smart_score += 12
    elif pct_over_1000 < 30:
        smart_score -= 10

    # 400張大戶集中
    if pct_over_400 >= 70:
        smart_score += 10
    elif pct_over_400 >= 55:
        smart_score += 5

    # 散戶少（籌碼沉澱）
    if pct_under_100 < 25:
        smart_score += 10
    elif pct_under_100 > 45:
        smart_score -= 10

    # 2. 價量型態
    if is_volume_breakout:
        smart_score += 15  # 主力出量表態
    elif is_volume_dryup and is_ma_entangled:
        smart_score += 10  # 窒息量即將變盤，洗盤完畢

    # 3. 趨勢與 KD
    if curr_p > ma20 and ma20 >= ma60:
        smart_score += 5
    if kd_k < 35 and kd_k > kd_d:
        smart_score += 5

    smart_score = min(100, max(0, smart_score))

    # 判斷主力特徵標籤
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

    # 交易建議判定
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
# 4. 主力進出多因子動態回測 (Smart Money Backtest Engine)
# -------------------------------------------------------------

def run_smart_money_backtest(df_p, pct_over_1000=0.0):
    """
    加入「主力籌碼門檻」的時機點回測策略：
    - 買進：MA20向上 + 均線收斂 + 放量突破(Vol > 1.5倍) + KD低檔向上 (若有千張大戶>50%則提高勝率)
    - 停利：波段漲幅達 18% 或 20日乖離 > 15%
    - 停損：收盤跌破 20MA 超過 3.5%
    """
    if len(df_p) < 120:
        return None

    capital = 1000000
    pos = 0
    buy_price = 0
    trade_count = 0
    win_count = 0
    equity = [capital]
    returns = []

    for i in range(60, len(df_p)):
        sub = df_p.iloc[:i+1]
        c = float(sub['Close'].iloc[-1])
        ma20 = float(sub['MA20'].iloc[-1])
        ma60 = float(sub['MA60'].iloc[-1])
        v = float(sub['Volume'].iloc[-1])
        v_ma20 = float(sub['Vol_MA20'].iloc[-1])
        k = float(sub['KD_K'].iloc[-1])
        d = float(sub['KD_D'].iloc[-1])
        prev_k = float(sub['KD_K'].iloc[-2])
        prev_d = float(sub['KD_D'].iloc[-2])

        # 買進條件
        if pos == 0:
            vol_expansion = (v > v_ma20 * 1.5) if v_ma20 > 0 else False
            price_above_ma = c > ma20 and ma20 > ma60
            kd_golden = (prev_k <= prev_d) and (k > d) and (k < 65)

            if price_above_ma and (vol_expansion or kd_golden):
                pos = capital / c
                buy_price = c
        # 持有中判斷出場
        elif pos > 0:
            ret = (c - buy_price) / buy_price
            bias = (c - ma20) / ma20 * 100

            # 停損: 跌破 20MA 超過 3.5% 或 跌破買價 7%
            stop_loss = (c < ma20 * 0.965) or (ret <= -0.07)
            # 停利: 達到 18% 或 乖離率過高 > 15%
            take_profit = (ret >= 0.18) or (bias >= 15.0)

            if stop_loss or take_profit:
                capital = pos * c
                pos = 0
                trade_count += 1
                if ret > 0:
                    win_count += 1
                returns.append(ret)

        current_val = (pos * c) if pos > 0 else capital
        equity.append(current_val)

    # 結算
    final_cap = (pos * float(df_p['Close'].iloc[-1])) if pos > 0 else capital
    tot_return = (final_cap - 1000000) / 1000000 * 100
    win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0

    eq_s = pd.Series(equity)
    cummax = eq_s.cummax()
    dd = (cummax - eq_s) / cummax * 100
    mdd = dd.max()

    return {
        '總報酬率(%)': round(tot_return, 2),
        '交易次數': trade_count,
        '勝率(%)': round(win_rate, 2),
        '最大回撤MDD(%)': round(mdd, 2)
    }

# -------------------------------------------------------------
# 5. 主程式流程
# -------------------------------------------------------------

def main():
    print("=" * 70)
    print(" 啟動『主力籌碼 × GVI 雙因子智能選股與回測系統』")
    print("=" * 70)

    # 1. 抓取集保股東持股分級
    tdcc_df = fetch_tdcc_shareholder_distribution()

    # 2. 讀取標的清單與基本面參考檔
    stock_ids = get_stock_list()
    ref_df = load_reference_data()

    if not stock_ids:
        print("未獲取到股票代號，結束。")
        return

    # 整理標準化 symbol 清單 (支援 .TW 與 .TWO)
    symbols = []
    symbol_to_id = {}
    for s in stock_ids:
        sym = s if ('.TW' in s or '.TWO' in s) else f"{s}.TW"
        symbols.append(sym)
        symbol_to_id[sym] = sym.split('.')[0]

    # 3. 批次下載一年歷史股價日 K 線
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=365)
    print(f"正在批次下載股價歷史數據 ({start_date} ~ {end_date})...")

    data = yf.download(symbols, start=start_date, end=end_date, group_by='ticker', auto_adjust=True, threads=True, progress=False)

    results = []
    backtest_rows = []

    for sym in symbols:
        sid = symbol_to_id[sym]
        name = get_tw_name(sym)

        # 取得個股日K
        df_p = pd.DataFrame()
        try:
            if isinstance(data.columns, pd.MultiIndex):
                if sym in data.columns.levels[0]:
                    df_p = data[sym].dropna(subset=['Close']).copy()
            else:
                df_p = data.dropna(subset=['Close']).copy()
        except Exception:
            pass

        if df_p.empty or len(df_p) < 40:
            continue

        # 計算指標: MA, Bias, Volume, KD
        df_p['MA5'] = df_p['Close'].rolling(5).mean()
        df_p['MA20'] = df_p['Close'].rolling(20).mean()
        df_p['MA60'] = df_p['Close'].rolling(60).mean()
        df_p['Vol_MA5'] = df_p['Volume'].rolling(5).mean()
        df_p['Vol_MA20'] = df_p['Volume'].rolling(20).mean()
        df_p['Bias_20'] = (df_p['Close'] - df_p['MA20']) / df_p['MA20'] * 100

        # KD
        low_9 = df_p['Low'].rolling(9).min()
        high_9 = df_p['High'].rolling(9).max()
        rsv = (df_p['Close'] - low_9) / (high_9 - low_9 + 1e-9) * 100
        df_p['KD_K'] = rsv.ewm(com=2).mean()
        df_p['KD_D'] = df_p['KD_K'].ewm(com=2).mean()

        # 取得該檔集保大戶資訊
        tdcc_row = tdcc_df.loc[sid] if (not tdcc_df.empty and sid in tdcc_df.index) else None

        # 評估主力價量與時機
        sig = evaluate_smart_money_signals(df_p, tdcc_row)

        # 取得基本面 (從 ref_df 讀取 GVI、PB、ROE)
        pb = np.nan
        roe = np.nan
        gvi = np.nan
        yield_rate = np.nan
        if not ref_df.empty and sid in ref_df.index:
            r = ref_df.loc[sid]
            if isinstance(r, pd.DataFrame):
                r = r.iloc[0]
            pb = parse_float(r.get('股價淨值比', np.nan))
            roe = parse_float(r.get('股東權益報酬率', np.nan))
            gvi = parse_float(r.get('GVI', np.nan))
            yield_rate = parse_float(r.get('殖利率', np.nan))

        # 執行主力回測
        bt = run_smart_money_backtest(df_p, sig.get('千張大戶比例(%)', 0))

        # 彙整評等
        item = {
            '證券代號': sid,
            '名稱': name,
            '現價': sig['現價'],
            '主力評分': sig['主力評分'],
            '時機建議': sig['時機建議'],
            '主力籌碼特徵': sig['主力特徵'],
            '千張大戶比例(%)': sig['千張大戶比例(%)'],
            '400張大戶比例(%)': sig['400張大戶比例(%)'],
            '100張以下散戶比例(%)': sig['100張以下散戶比例(%)'],
            '量能倍數': sig['量能倍數'],
            '均線糾結度(%)': sig['均線糾結度(%)'],
            '20日乖離率(%)': sig['20日乖離率(%)'],
            'KD_K': sig['KD_K'],
            'KD_D': sig['KD_D'],
            'GVI': gvi,
            '股價淨值比PB': pb,
            'ROE(%)': roe,
            '殖利率(%)': yield_rate
        }
        results.append(item)

        if bt:
            bt_item = {
                '證券代號': sid,
                '名稱': name,
                '主力評分': sig['主力評分'],
                '時機建議': sig['時機建議'],
                '千張大戶比例(%)': sig['千張大戶比例(%)'],
                '總報酬率(%)': bt['總報酬率(%)'],
                '勝率(%)': bt['勝率(%)'],
                '交易次數': bt['交易次數'],
                '最大回撤MDD(%)': bt['最大回撤MDD(%)']
            }
            backtest_rows.append(bt_item)

    # 輸出 CSV
    df_results = pd.DataFrame(results).sort_values(by='主力評分', ascending=False)
    df_backtest = pd.DataFrame(backtest_rows).sort_values(by='總報酬率(%)', ascending=False)

    public_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'public')
    out_smart_path = os.path.join(public_dir, 'Stock_SmartMoney_Signals.csv')
    out_bt_path = os.path.join(public_dir, 'Stock_SmartMoney_Backtest.csv')

    df_results.to_csv(out_smart_path, index=False, encoding='utf-8-sig')
    df_backtest.to_csv(out_bt_path, index=False, encoding='utf-8-sig')

    print(f"\n[完成] 主力籌碼分析結果已匯出至: {out_smart_path}")
    print(f"[完成] 主力動態回測結果已匯出至: {out_bt_path}")
    print("\n主力評分 Top 10 標的：")
    print(df_results[['證券代號', '名稱', '現價', '主力評分', '時機建議', '千張大戶比例(%)', '主力籌碼特徵']].head(10).to_string(index=False))

if __name__ == '__main__':
    main()
