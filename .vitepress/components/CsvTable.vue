<template>
  <div class="table-container">
    <table v-if="data.length">
      <thead>
        <tr>
          <th v-for="(header, index) in headers" :key="index">{{ header }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, rowIndex) in data" :key="rowIndex">
          <td v-for="(cell, cellIndex) in row" :class="{ numeric: isNumeric(cell) }" :key="cellIndex">{{ cell }}</td>
        </tr>
      </tbody>
    </table>
    <div v-else-if="loading">Loading...</div>
    <div v-else>Error loading data</div>
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
      error: false
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
      } catch (error) {
        console.error('Error loading CSV:', error);
        this.error = true;
      } finally {
        this.loading = false;
      }
    },
    isNumeric(value) {
      return !isNaN(parseFloat(value)) && isFinite(value);
    }
  }
};
</script>

<style>

.table-container table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed; 
}

.table-container th,
.table-container td {
  white-space: nowrap;
}

.table-container th {
  position: sticky;
  top: 0;
  z-index: 1;
}

.numeric {
  text-align: right;
}
</style>