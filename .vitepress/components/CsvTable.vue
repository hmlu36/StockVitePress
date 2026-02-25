<template>
    <div class="table-wrapper">
        <!-- 載入狀態 -->
        <div v-if="loading" class="loading">Loading...</div>
        <div v-else-if="error" class="error">Error loading data</div>

        <!-- 單一表格結構 -->
        <div v-else class="table-scroll-container">
            <table class="sticky-table">
                <thead>
                    <tr>
                        <th v-for="(header, index) in headers" :key="index"
                            :class="{ 'sticky-col': index < fixedColumns, 'first-col': index === 0 }"
                            :style="getStickyStyle(index, true)">
                            {{ header }}
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for="(row, rowIndex) in data" :key="rowIndex">
                        <td v-for="(cell, cellIndex) in row" :key="cellIndex" :class="{
                            numeric: isNumeric(cell),
                            'sticky-col': cellIndex < fixedColumns,
                            'first-col': cellIndex === 0
                        }" :style="getStickyStyle(cellIndex)">
                            {{ cell }}
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</template>

<script>
import Papa from 'papaparse';

export default {
    props: {
        csvFilePath: {
            type: String,
            required: true
        },
        fixedColumns: {
            type: Number,
            default: 1
        }
    },
    data() {
        return {
            headers: [],
            data: [],
            loading: true,
            error: false,
            columnWidths: []
        };
    },
    mounted() {
        this.loadCsv();
        window.addEventListener('resize', this.calculateColumnWidths);
    },
    beforeUnmount() {
        window.removeEventListener('resize', this.calculateColumnWidths);
    },
    methods: {
        async loadCsv() {
            try {
                const isProduction = process.env.NODE_ENV === 'production';
                const BASE_URL = isProduction ? '/StockVitePress' : '';
                const csvPath = `${BASE_URL}/${this.csvFilePath}`;
                const response = await fetch(csvPath);
                if (!response.ok) throw new Error('Network response was not ok');

                const csvText = await response.text();
                const parsed = Papa.parse(csvText, { header: true });
                this.headers = parsed.meta.fields;
                this.data = parsed.data;

                this.$nextTick(() => {
                    this.calculateColumnWidths();
                });
            } catch (error) {
                console.error('Error loading CSV:', error);
                this.error = true;
            } finally {
                this.loading = false;
            }
        },
        isNumeric(value) {
            return !isNaN(parseFloat(value)) && isFinite(value);
        },
        calculateColumnWidths() {
            const table = this.$el.querySelector('table');
            if (!table) return;

            // 計算前幾欄的寬度，用於計算 sticky 的 left 位置
            const firstRowCells = table.querySelectorAll('thead th');
            this.columnWidths = Array.from(firstRowCells).map(cell => cell.offsetWidth); // 用 offsetWidth 包含 border/padding
        },
        getStickyStyle(index, isHeader = false) {
            if (index >= this.fixedColumns) return {};

            // 計算 left 偏移量
            let left = 0;
            for (let i = 0; i < index; i++) {
                left += this.columnWidths[i] || 0;
            }

            const style = {
                left: `${left}px`,
                position: 'sticky',
            };

            // Z-Index 層級管理
            if (isHeader) {
                style.zIndex = 3; // 左上角 (Header + Sticky Col)
            } else {
                style.zIndex = 1; // Body 的 Sticky Col
            }

            return style;
        }
    }
};
</script>

<style scoped>
.table-wrapper {
    /* 確保外部容器有明確高度限制，這樣內部才能捲動 */
    height: calc(100vh - 160px);
    border: 1px solid var(--vp-c-divider);
    border-radius: 8px;
    overflow: hidden;
    background-color: var(--vp-c-bg);
    display: flex;
    flex-direction: column;
    margin-top: 0;

    /* 突破 VitePress 內容區域寬度限制，讓表格延伸到全寬 */
    width: 100%;
    max-width: none;
}

/* 當側邊欄隱藏時（小螢幕或無側邊欄頁面） */
@media (max-width: 959px) {
    .table-wrapper {
        width: 100%;
    }
}

.table-scroll-container {
    overflow: auto;
    flex: 1;
    width: 100%;
    height: 100%;
    position: relative;

    /* 美化捲軸 */
    scrollbar-width: thin;
    scrollbar-color: var(--vp-c-divider) var(--vp-c-bg-soft);
}

.table-scroll-container::-webkit-scrollbar {
    width: 12px;
    height: 12px;
}

.table-scroll-container::-webkit-scrollbar-track {
    background: var(--vp-c-bg-soft);
}

.table-scroll-container::-webkit-scrollbar-thumb {
    background: var(--vp-c-divider);
    border-radius: 6px;
    border: 2px solid var(--vp-c-bg-soft);
}

.table-scroll-container::-webkit-scrollbar-corner {
    background: var(--vp-c-bg-soft);
}

.sticky-table {
    border-collapse: separate;
    border-spacing: 0;
    min-width: 100%;
    width: max-content;
}

/* ----------- Header Sticky ----------- */
.sticky-table thead th {
    position: sticky !important;
    /* 強制生效 */
    top: 0 !important;
    z-index: 10;
    /* 提高 z-index */
    background-color: var(--vp-c-bg-soft);
    box-shadow: 0 1px 0 var(--vp-c-divider);
    border-bottom: none;
}

/* ----------- Column Sticky ----------- */
.sticky-col {
    position: sticky !important;
    /* 強制生效 */
    left: 0;
    /* JS 會覆蓋這個，但預設給 0 */
    z-index: 5;
    background-color: var(--vp-c-bg-soft);
    box-shadow: 1px 0 0 var(--vp-c-divider);
    border-right: none !important;
}

/* Header 和 Sticky Col 交界處 (左上角) */
.sticky-table thead th.sticky-col {
    z-index: 20 !important;
    /* 最高層級 */
    box-shadow: 1px 0 0 var(--vp-c-divider), 0 1px 0 var(--vp-c-divider);
}

/* 確保第一欄（若不是 sticky-col）也有基本樣式 */
.sticky-table th,
.sticky-table td {
    padding: 12px 16px;
    white-space: nowrap;
    border-right: 1px solid var(--vp-c-divider-light);
    border-bottom: 1px solid var(--vp-c-divider-light);
    box-sizing: border-box;
}

.sticky-table td {
    background-color: var(--vp-c-bg);
}

/* 固定欄在 tbody 裡的背景 */
.sticky-table tbody td.sticky-col {
    background-color: var(--vp-c-bg-soft);
}

.sticky-table tbody tr:hover td {
    background-color: var(--vp-c-bg-mute);
}

.sticky-table th:last-child,
.sticky-table td:last-child {
    border-right: none;
}

.sticky-table tbody tr:last-child td {
    border-bottom: none;
}

.numeric {
    text-align: right;
    font-variant-numeric: tabular-nums;
}

.loading,
.error {
    padding: 20px;
    text-align: center;
    color: var(--vp-c-text-2);
    height: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
}
</style>

<!-- 非 scoped 樣式：打破 VitePress 內容區域寬度限制 -->
<style>
/* 展開所有 VitePress 內容容器至全寬 */
.VPDoc .container,
.VPDoc .content,
.VPDoc .content-container,
.vp-doc,
.vp-doc .container,
.VPContent {
  max-width: 100% !important;
  width: 100% !important;
}

.vp-doc {
  padding-left: 24px !important;
  padding-right: 24px !important;
}

/* 移除 VitePress 自動包裹 <p> 或 <div> 產生的上方空白 */
.vp-doc p:has(> .table-wrapper),
.vp-doc div:has(> .table-wrapper) {
  margin: 0 !important;
  padding: 0 !important;
  line-height: 0;
}

/* 消除 h1 之後的間距 */
.vp-doc h1 + div .table-wrapper,
.vp-doc h1 + p .table-wrapper {
  margin-top: 0 !important;
}
</style>