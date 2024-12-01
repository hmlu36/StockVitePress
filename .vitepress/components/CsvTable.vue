<template>
  <div>
    <table v-if="data.length">
      <thead>
        <tr>
          <th v-for="(header, index) in headers" :key="index">{{ header }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, rowIndex) in data" :key="rowIndex">
          <td v-for="(cell, cellIndex) in row" :key="cellIndex">{{ cell }}</td>
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
    }
  }
};
</script>

<style>
table {
  width: 100%;
  border-collapse: collapse;
}

th,
td {
  border: 1px solid #ddd;
  padding: 8px;
}

th {
  background-color: #f2f2f2;
}
</style>