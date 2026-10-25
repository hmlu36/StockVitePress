<script setup>
import CsvTable from '/.vitepress/components/CsvTable.vue';
import BacktestCharts from '/.vitepress/components/BacktestCharts.vue';
</script>

# 雙因子動態回測與買賣時機點 Dashboard

> 本儀表板依據 **GVI 估值 + 財務體質** 進行第一道安全邊際篩選，並以 **20MA（月線）與帶量突破** 作為波段進出場的時機點，透過歷史近 3 年價量數據完成回測驗證。

---

## 圖表分析總覽

<BacktestCharts />

---

## 1. 即時買賣時機點與決策儀表板

<CsvTable csvFilePath="Stock_Backtest_Signals.csv" :fixedColumns="2" />

---

## 2. 歷史 3 年波段策略回測績效排行榜

<CsvTable csvFilePath="Stock_Backtest_Performance.csv" :fixedColumns="2" />

---

## 策略參數說明

| 參數 | 設定值 | 說明 |
|:---|:---|:---|
| 回測期間 | 近 3 年 | 日線還原權息 (Auto-Adjusted) 歷史數據 |
| 買進時機 | 20MA 穿越 / 帶量突破近 20 日高點 | 量能門檻: > 1.2 倍均量 |
| 停損 | -8% | 進場後跌幅達到 -8% 無條件停損 |
| 移動停利 | 跌破 20MA (收盤) | 連續 2 日跌破或單日重跌 2.5% 以上 |
| 波段鎖利 | +35% 且跌破 5MA | 獲利達標後跌破短線均線時先鎖利 |
| 流動性門檻 | 20 日均量 >= 300 張 | 取代原本「換手率 <5%」的錯誤設定 |
