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
                        <th v-for="(header, index) in headers" :key="index" class="sortable"
                            :class="{ 'sticky-col': index < fixedColumns, 'first-col': index === 0, sorted: sortIndex === index }"
                            :style="getStickyStyle(index, true)"
                            :aria-sort="sortIndex === index ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'"
                            :title="`點擊依「${header}」排序`"
                            @click="sortBy(index)">
                            <span class="th-label">{{ header }}</span>
                            <span class="sort-indicator">{{ sortIndex === index ? (sortDir === 'asc' ? '▲' : '▼') : '↕' }}</span>
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
            originalData: [],
            loading: true,
            error: false,
            columnWidths: [],
            sortIndex: null,
            sortDir: 'asc'
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
                // import.meta.env.BASE_URL 由 VitePress config 的 base 自動決定
                // dev: '/'  production(GitHub Pages): '/StockVitePress/'
                const csvPath = `${import.meta.env.BASE_URL}${this.csvFilePath}`;
                const response = await fetch(csvPath);
                if (!response.ok) throw new Error('Network response was not ok');

                const csvText = await response.text();
                const parsed = Papa.parse(csvText, { header: true });
                this.headers = parsed.meta.fields;
                this.originalData = parsed.data;
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
        sortBy(index) {
            if (this.sortIndex !== index) {
                this.sortIndex = index;
                this.sortDir = 'asc';
            } else if (this.sortDir === 'asc') {
                this.sortDir = 'desc';
            } else {
                this.sortIndex = null;
                this.sortDir = 'asc';
            }
            this.applySort();
        },
        applySort() {
            if (this.sortIndex === null) {
                this.data = [...this.originalData];
            } else {
                const key = this.headers[this.sortIndex];
                const dir = this.sortDir === 'asc' ? 1 : -1;
                const values = this.originalData
                    .map(row => this.parseSortValue(row[key]))
                    .filter(value => value !== null);
                const isNumericColumn = values.length > 0
                    && values.every(value => typeof value === 'number');

                this.data = [...this.originalData].sort((a, b) => {
                    const av = this.parseSortValue(a[key]);
                    const bv = this.parseSortValue(b[key]);
                    // 空值一律排在最後
                    if (av === null && bv === null) return 0;
                    if (av === null) return 1;
                    if (bv === null) return -1;

                    if (isNumericColumn) return (av - bv) * dir;
                    return String(av).localeCompare(String(bv), 'zh-Hant', { numeric: true }) * dir;
                });
            }

            this.$nextTick(() => {
                this.calculateColumnWidths();
            });
        },
        parseSortValue(raw) {
            if (raw === undefined || raw === null) return null;
            const text = String(raw).trim();
            if (!text || text === '-' || text === '—') return null;

            const cleaned = text.replace(/,/g, '');
            const match = cleaned.match(/^[+-]?\d+(?:\.\d+)?/);
            if (!match) return text;

            let value = parseFloat(match[0]);
            const unit = cleaned.slice(match[0].length);
            if (unit.startsWith('億')) value *= 1e8;
            else if (unit.startsWith('萬')) value *= 1e4;
            else if (unit.startsWith('千')) value *= 1e3;
            return value;
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
    /* 維持表格緊湊列高 */
    line-height: 0;
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

/* ----------- 欄位排序 ----------- */
.sticky-table thead th.sortable {
    cursor: pointer;
    user-select: none;
}

.sticky-table thead th.sortable:hover {
    background-color: var(--vp-c-bg-mute);
}

.sort-indicator {
    display: inline-block;
    width: 1em;
    margin-left: 4px;
    font-size: 0.75em;
    opacity: 0.35;
}

.sticky-table thead th.sorted .sort-indicator {
    opacity: 1;
    color: var(--vp-c-brand-1);
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

/* 消除 h1 之後的間距 */
.vp-doc h1 + div .table-wrapper,
.vp-doc h1 + p .table-wrapper {
  margin-top: 0 !important;
}

/* 含表格的頁面：隱藏右側大綱並移除右側留白，讓表格延伸到視窗最右緣 */
@media (min-width: 960px) {
  .VPDoc:has(.table-wrapper) .aside {
    display: none !important;
  }

  .VPDoc:has(.table-wrapper),
  .VPDoc:has(.table-wrapper) .content,
  .vp-doc:has(.table-wrapper) {
    padding-right: 0 !important;
  }
}
</style>