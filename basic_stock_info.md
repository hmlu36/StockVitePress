
<script setup>
import CsvTable from '/.vitepress/components/CsvTable.vue';
</script>

### 資料來源與篩選條件

| 來源 | 抓取內容 / 篩選條件 |
| :--- | :--- |
| **證券交易所 (TWSE)** | 收盤價、本益比 (< 10)、殖利率 (> 8%)、淨值比 |
| **公開資訊觀測站 (MOPS)** | 資本額、上市日期 (> 5 年)、營益分析 (毛利、營業利益率等) |
| **集保結算所 (TDCC)** | 股東分級資料、計算散戶 (< 100張) 與大戶 (> 1000張) 比例 |
| **董監持股 (Norway)** | 追蹤董監事持股成數及其變動趨勢 |

# 篩選基本資料
<CsvTable csvFilePath="basic_stock_info.csv" />