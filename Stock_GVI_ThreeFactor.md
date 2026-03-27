# 抓取與分析邏輯說明

### 1. 資料來源

| 來源 | 說明 |
| :--- | :--- |
| **yfinance** | 抓取歷史價量、資產負債表 (BS)、現金流量表 (CF) 與損益表 (IS) |
| **自定義資料** | 整合資本額、每股淨值等參考數據 |

### 2. 核心評等指標

| 指標 | 說明 |
| :--- | :--- |
| **GVI 指標** | (1/淨值比) * (1 + ROE)^n，衡量估值與長期成長性 |
| **三因子評分** | 綜合 ROE、PB (淨值比) 與 近20日報酬率的百分比排名 (Ranking) |
| **財務健康** | 評估自由現金流、負債比、流動比、本業收益比例與現金流量比 |
| **由三好一公道** | ROE >= 12%、低淨負債、EPS 成長與本益比策略 (PE <= 20) |
| **四道關卡** | 獲利能力、財務安全、成長動能與估值狀態（便宜/合理/昂貴）分析 |

<script setup>
import CsvTable from '/.vitepress/components/CsvTable.vue';
</script>

# GVI + 三因子評分
<CsvTable csvFilePath="Stock_GVI_ThreeFactor.csv" />