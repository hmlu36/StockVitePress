<script setup>
import CsvTable from '/.vitepress/components/CsvTable.vue';
</script>

<style>
.vp-doc .container {
  max-width: 100% !important;
}

.vp-doc {
  max-width: 100% !important;
  padding-left: 24px !important;
  padding-right: 24px !important;
}

.table-container {
  width: 100%;
  overflow-x: auto;
}
</style>

# GVI + 三因子評分
<CsvTable csvFilePath="Stock_GVI_ThreeFactor.csv" />