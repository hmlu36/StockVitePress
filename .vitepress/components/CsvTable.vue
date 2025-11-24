<template>
  <div class="table-wrapper">
    <div class="table-scroll-container">
      <table v-if="data.length">
        <thead>
          <tr>
            <th 
              v-for="(header, index) in headers" 
              :key="index"
              :class="{ 'sticky-col': index < 2 }"
              :style="getStickyStyle(index)"
            >
              {{ header }}
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, rowIndex) in data" :key="rowIndex">
            <td 
              v-for="(cell, cellIndex) in row" 
              :key="cellIndex"
              :class="{ 
                numeric: isNumeric(cell),
                'sticky-col': cellIndex < 2
              }"
              :style="getStickyStyle(cellIndex)"
            >
              {{ cell }}
            </td>
          </tr>
        </tbody>
      </table>
      <div v-else-if="loading" class="loading">Loading...</div>
      <div v-else class="error">Error loading data</div>
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
  },
  methods: {
    async loadCsv() {
      try {
        const isProduction = process.env.NODE_ENV === 'production';
        const BASE_URL = isProduction ? '/StockVitePress' : '';
        const csvPath = `${BASE_URL}/${this.csvFilePath}`;
        const response = await fetch(csvPath);
        if (!response.ok) {
          throw new Error('Network response was not ok');
        }
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
      if (table) {
        const firstRow = table.querySelector('thead tr');
        if (firstRow) {
          const cells = firstRow.querySelectorAll('th');
          this.columnWidths = Array.from(cells).map(cell => cell.offsetWidth);
        }
      }
    },
    getStickyStyle(index) {
      if (index === 0) {
        return {
          left: '0px',
          zIndex: 3
        };
      } else if (index === 1) {
        const leftOffset = this.columnWidths[0] || 0;
        return {
          left: `${leftOffset}px`,
          zIndex: 3
        };
      }
      return {};
    },
  }
};
</script>

<style scoped>
.table-wrapper {
  /* 設定固定高度，讓捲軸限制在容器內 */
  height: calc(100vh - 160px);
  display: flex;
  flex-direction: column;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  overflow: hidden;
  background-color: var(--vp-c-bg);
}

.table-scroll-container {
  flex: 1;
  overflow: auto; /* 自動顯示垂直與水平捲軸 */
  position: relative;
  
  /* 美化捲軸 */
  scrollbar-width: thin;
  scrollbar-color: var(--vp-c-divider) var(--vp-c-bg-soft);
}

.table-scroll-container::-webkit-scrollbar {
  width: 12px;
  height: 12px; /* 恢復水平捲軸高度 */
}

.table-scroll-container::-webkit-scrollbar-track {
  background: var(--vp-c-bg-soft);
}

.table-scroll-container::-webkit-scrollbar-thumb {
  background: var(--vp-c-divider);
  border-radius: 6px;
  border: 2px solid var(--vp-c-bg-soft);
}

.table-scroll-container::-webkit-scrollbar-thumb:hover {
  background: var(--vp-c-text-3);
}

/* 表格角落的方塊（當雙向捲軸都出現時） */
.table-scroll-container::-webkit-scrollbar-corner {
  background: var(--vp-c-bg-soft);
}

table {
  border-collapse: separate;
  border-spacing: 0;
  width: max-content;
  min-width: 100%;
}

thead {
  position: sticky;
  top: 0;
  z-index: 2;
}

th {
  padding: 12px 16px;
  text-align: left;
  font-weight: 600;
  border-bottom: 2px solid var(--vp-c-divider);
  background-color: var(--vp-c-bg-soft);
  white-space: nowrap;
  position: sticky;
  top: 0;
}

td {
  padding: 10px 16px;
  border-bottom: 1px solid var(--vp-c-divider-light);
  white-space: nowrap;
  background-color: var(--vp-c-bg);
}

/* 固定列樣式 */
.sticky-col {
  position: sticky !important;
  background-color: var(--vp-c-bg-soft) !important;
  box-shadow: 2px 0 4px rgba(0, 0, 0, 0.1);
}

thead .sticky-col {
  z-index: 4 !important;
  background-color: var(--vp-c-bg-soft) !important;
}

tbody tr:hover td {
  background-color: var(--vp-c-bg-mute);
}

tbody tr:hover .sticky-col {
  background-color: var(--vp-c-bg-mute) !important;
}

.numeric {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.loading, .error {
  padding: 20px;
  text-align: center;
  color: var(--vp-c-text-2);
}
</style>