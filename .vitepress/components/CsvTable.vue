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
    <div v-else>Loading...</div>
  </div>
</template>

<script>
import Papa from 'papaparse';

export default {
  data() {
    return {
      headers: [],
      data: []
    };
  },
  mounted() {
    this.loadCsv();
  },
  methods: {
    loadCsv() {
      const csvPath = '/release.csv';
      fetch(csvPath)
        .then(response => response.text())
        .then(csvText => {
          const parsed = Papa.parse(csvText, { header: true });
          this.headers = parsed.meta.fields;
          this.data = parsed.data;
        });
    }
  }
};
</script>

<style>
table {
  width: 100%;
  border-collapse: collapse;
}
th, td {
  border: 1px solid #ddd;
  padding: 8px;
}
th {
  background-color: #f2f2f2;
}
</style>