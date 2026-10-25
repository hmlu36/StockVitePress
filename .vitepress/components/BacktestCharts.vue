<template>
  <div class="charts-dashboard">
    <div v-if="loading" class="charts-loading">
      <div class="spinner"></div>
      <span>載入圖表數據中...</span>
    </div>
    <template v-else>
      <div class="kpi-row">
        <div class="kpi-card buy">
          <div class="kpi-icon">+</div>
          <div class="kpi-content">
            <div class="kpi-value">{{ kpi.buyCount }}</div>
            <div class="kpi-label">今日買進訊號</div>
          </div>
        </div>
        <div class="kpi-card hold">
          <div class="kpi-icon">H</div>
          <div class="kpi-content">
            <div class="kpi-value">{{ kpi.holdCount }}</div>
            <div class="kpi-label">多頭持股 / 續抱</div>
          </div>
        </div>
        <div class="kpi-card sell">
          <div class="kpi-icon">-</div>
          <div class="kpi-content">
            <div class="kpi-value">{{ kpi.sellCount }}</div>
            <div class="kpi-label">賣出 / 警戒</div>
          </div>
        </div>
        <div class="kpi-card watch">
          <div class="kpi-icon">?</div>
          <div class="kpi-content">
            <div class="kpi-value">{{ kpi.watchCount }}</div>
            <div class="kpi-label">觀望中</div>
          </div>
        </div>
        <div class="kpi-card perf">
          <div class="kpi-icon">*</div>
          <div class="kpi-content">
            <div class="kpi-value">{{ kpi.topCagr }}%</div>
            <div class="kpi-label">最高年化報酬 ({{ kpi.topName }})</div>
          </div>
        </div>
        <div class="kpi-card win">
          <div class="kpi-icon">%</div>
          <div class="kpi-content">
            <div class="kpi-value">{{ kpi.avgWinRate }}%</div>
            <div class="kpi-label">平均勝率 (全市場)</div>
          </div>
        </div>
      </div>
      <div class="charts-grid-2">
        <div class="chart-card">
          <div class="chart-title">回測年化報酬率 Top 20 <span class="chart-sub">(3年 · 20MA策略)</span></div>
          <canvas ref="barChartRef"></canvas>
        </div>
        <div class="chart-card">
          <div class="chart-title">風險 vs 報酬分析 <span class="chart-sub">(X:MDD · Y:年化報酬率 · 大小:勝率)</span></div>
          <canvas ref="bubbleChartRef"></canvas>
        </div>
      </div>
      <div class="charts-grid-2">
        <div class="chart-card">
          <div class="chart-title">今日市場決策分布 <span class="chart-sub">({{ signalTotal }} 檔)</span></div>
          <canvas ref="donutChartRef"></canvas>
        </div>
        <div class="chart-card">
          <div class="chart-title">ROE vs GVI 基本面分析 <span class="chart-sub">(顏色=體質評等)</span></div>
          <canvas ref="scatterChartRef"></canvas>
        </div>
      </div>
      <div class="chart-card chart-card-full">
        <div class="chart-title">勝率 vs 盈虧比 綜合評估 <span class="chart-sub">(All {{ perfData.length }} 檔 · 氣泡大小=年化報酬率)</span></div>
        <canvas ref="scatterWinRef"></canvas>
      </div>
    </template>
  </div>
</template>

<script>
import Papa from 'papaparse';

let ChartJS = null;

function parseNum(str) {
  if (!str || str === '-') return null;
  const cleaned = String(str).replace(/%|x/g, '').trim();
  const n = parseFloat(cleaned);
  return isNaN(n) ? null : n;
}

export default {
  name: 'BacktestCharts',
  props: {
    signalsCsvPath: { type: String, default: 'Stock_Backtest_Signals.csv' },
    perfCsvPath:    { type: String, default: 'Stock_Backtest_Performance.csv' },
  },
  data() {
    return {
      loading: true,
      signalData: [],
      perfData: [],
      kpi: { buyCount: 0, holdCount: 0, sellCount: 0, watchCount: 0, topCagr: 0, topName: '-', avgWinRate: 0 },
      signalTotal: 0,
    };
  },
  async mounted() {
    if (typeof window !== 'undefined') {
      await import('https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js');
      ChartJS = window.Chart;
    }
    await this.loadData();
    this.loading = false;
    await this.$nextTick();
    this.drawAll();
  },
  beforeUnmount() {
    ['barChartRef','bubbleChartRef','donutChartRef','scatterChartRef','scatterWinRef'].forEach(ref => {
      if (ChartJS && this.$refs[ref]) {
        const c = ChartJS.getChart(this.$refs[ref]);
        if (c) c.destroy();
      }
    });
  },
  methods: {
    async fetchCsv(path) {
      const url = `${import.meta.env.BASE_URL}${path}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error('fetch fail: ' + path);
      const text = await res.text();
      return Papa.parse(text, { header: true, skipEmptyLines: true }).data;
    },
    async loadData() {
      try {
        [this.signalData, this.perfData] = await Promise.all([
          this.fetchCsv(this.signalsCsvPath),
          this.fetchCsv(this.perfCsvPath),
        ]);
        this.computeKpi();
      } catch(e) { console.error('chart load error', e); }
    },
    computeKpi() {
      const s = this.signalData;
      this.signalTotal = s.length;
      this.kpi.buyCount  = s.filter(r => (r['決策建議']||'').includes('買進')).length;
      this.kpi.holdCount = s.filter(r => (r['決策建議']||'').includes('持股') || (r['決策建議']||'').includes('續抱')).length;
      this.kpi.sellCount = s.filter(r => (r['決策建議']||'').includes('賣出') || (r['決策建議']||'').includes('獲利') || (r['決策建議']||'').includes('出場')).length;
      this.kpi.watchCount= s.filter(r => (r['決策建議']||'').includes('觀望') || (r['決策建議']||'').includes('濾網')).length;
      const p = this.perfData;
      if (p.length > 0) {
        this.kpi.topCagr = parseNum(p[0]['年化報酬率(CAGR)']) ?? 0;
        this.kpi.topName = p[0]['證券名稱'] ?? '-';
        const rates = p.map(r => parseNum(r['勝率'])).filter(v => v !== null);
        this.kpi.avgWinRate = rates.length ? (rates.reduce((a,b)=>a+b,0)/rates.length).toFixed(1) : 0;
      }
    },
    theme() {
      const dark = document.documentElement.classList.contains('dark');
      return {
        text:  dark ? '#e2e8f0' : '#1e293b',
        grid:  dark ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.08)',
        bg:    dark ? '#1e293b' : '#ffffff',
        label: dark ? '#94a3b8' : '#64748b',
      };
    },
    destroyChart(refName) {
      if (!ChartJS || !this.$refs[refName]) return;
      const c = ChartJS.getChart(this.$refs[refName]);
      if (c) c.destroy();
    },
    drawAll() {
      if (!ChartJS) return;
      this.drawBar();
      this.drawBubble();
      this.drawDonut();
      this.drawRoeGvi();
      this.drawWinRate();
    },
    drawBar() {
      const t = this.theme();
      const top = this.perfData.slice(0, 20);
      const labels = top.map(r => r['證券名稱'] || r['證券代號']);
      const cagrs  = top.map(r => parseNum(r['年化報酬率(CAGR)']) ?? 0);
      const mdds   = top.map(r => parseNum(r['最大回撤(MDD)']) ?? 0);
      const colorFn = v => v >= 50 ? '#22c55e' : v >= 25 ? '#3b82f6' : v >= 10 ? '#f59e0b' : '#ef4444';
      this.destroyChart('barChartRef');
      new ChartJS(this.$refs.barChartRef, {
        type: 'bar',
        data: {
          labels,
          datasets: [
            { label: '年化報酬率(%)', data: cagrs, backgroundColor: cagrs.map(colorFn), borderRadius: 6, borderSkipped: false },
            { label: '最大回撤(%)', data: mdds.map(v=>-v), backgroundColor: 'rgba(239,68,68,0.35)', borderColor:'#ef4444', borderWidth:1, borderRadius:4, borderSkipped:false },
          ],
        },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { labels: { color: t.text, font:{size:11} } },
            tooltip: { callbacks: { label: ctx => ctx.datasetIndex===1 ? `MDD: ${Math.abs(ctx.raw).toFixed(1)}%` : `CAGR: ${ctx.raw.toFixed(1)}%` } },
          },
          scales: {
            x: { ticks:{ color:t.label, callback:v=>`${v}%` }, grid:{color:t.grid} },
            y: { ticks:{ color:t.text, font:{size:10} }, grid:{display:false} },
          },
        },
      });
    },
    drawBubble() {
      const t = this.theme();
      const pts = this.perfData.map(r => ({
        x: parseNum(r['最大回撤(MDD)']),
        y: parseNum(r['年化報酬率(CAGR)']),
        r: Math.max(4, (parseNum(r['勝率'])||30)/8),
        label: r['證券名稱']||r['證券代號'],
        wr: parseNum(r['勝率'])||0,
      })).filter(p => p.x!==null && p.y!==null);
      const cf = p => p.y>30?'rgba(34,197,94,0.7)':p.y>10?'rgba(59,130,246,0.7)':'rgba(239,68,68,0.6)';
      this.destroyChart('bubbleChartRef');
      new ChartJS(this.$refs.bubbleChartRef, {
        type: 'bubble',
        data: { datasets: [{ label:'各股', data:pts, backgroundColor:pts.map(cf), borderColor:pts.map(p=>p.y>30?'#16a34a':p.y>10?'#2563eb':'#dc2626'), borderWidth:1 }] },
        options: {
          responsive:true, maintainAspectRatio:false,
          plugins: {
            legend:{display:false},
            tooltip:{callbacks:{label:ctx=>[`${ctx.raw.label}`,`CAGR: ${ctx.raw.y.toFixed(1)}%`,`MDD: ${ctx.raw.x.toFixed(1)}%`,`勝率: ${ctx.raw.wr.toFixed(1)}%`]}},
          },
          scales: {
            x:{ title:{display:true,text:'最大回撤 MDD (%)',color:t.label}, ticks:{color:t.label,callback:v=>`${v}%`}, grid:{color:t.grid} },
            y:{ title:{display:true,text:'年化報酬率 CAGR (%)',color:t.label}, ticks:{color:t.label,callback:v=>`${v}%`}, grid:{color:t.grid} },
          },
        },
      });
    },
    drawDonut() {
      const t = this.theme();
      const cat = { '強烈買進':0, '多頭持股/續抱':0, '靜待買點':0, '獲利/警戒':0, '出場/賣出':0, '觀望/濾網':0 };
      this.signalData.forEach(r => {
        const d = r['決策建議']||'';
        if (d.includes('買進') && d.includes('強烈')) cat['強烈買進']++;
        else if (d.includes('持股')||d.includes('續抱')) cat['多頭持股/續抱']++;
        else if (d.includes('靜待')||d.includes('良好')) cat['靜待買點']++;
        else if (d.includes('獲利')||d.includes('警戒')) cat['獲利/警戒']++;
        else if (d.includes('出場')||d.includes('賣出')) cat['出場/賣出']++;
        else cat['觀望/濾網']++;
      });
      this.destroyChart('donutChartRef');
      new ChartJS(this.$refs.donutChartRef, {
        type: 'doughnut',
        data: {
          labels: Object.keys(cat),
          datasets:[{ data:Object.values(cat), backgroundColor:['#22c55e','#3b82f6','#6366f1','#f59e0b','#ef4444','#64748b'], hoverOffset:12, borderWidth:2, borderColor:t.bg }],
        },
        options: {
          responsive:true, maintainAspectRatio:false, cutout:'62%',
          plugins:{
            legend:{position:'right',labels:{color:t.text,font:{size:12},padding:14}},
            tooltip:{callbacks:{label:ctx=>`${ctx.label}: ${ctx.parsed}檔 (${((ctx.parsed/this.signalTotal)*100).toFixed(1)}%)`}},
          },
        },
      });
    },
    drawRoeGvi() {
      const t = this.theme();
      const cm = {'優良':'#22c55e','尚可':'#f59e0b','偏弱':'#ef4444','合格':'#3b82f6'};
      const grp = {};
      this.signalData.forEach(r => {
        const roe = parseNum(r['ROE']), gvi = parseNum(r['GVI指標']);
        if (roe===null || gvi===null || gvi<=0) return;
        const g = r['體質評等']||'合格';
        if (!grp[g]) grp[g]=[];
        grp[g].push({ x:roe, y:gvi, label:r['證券名稱']||r['證券代號'] });
      });
      const datasets = Object.entries(grp).map(([g, pts]) => ({
        label:`體質：${g}`, data:pts, backgroundColor:cm[g]||'#64748b', pointRadius:7, pointHoverRadius:10,
      }));
      this.destroyChart('scatterChartRef');
      new ChartJS(this.$refs.scatterChartRef, {
        type:'scatter', data:{datasets},
        options:{
          responsive:true, maintainAspectRatio:false,
          plugins:{
            legend:{labels:{color:t.text,font:{size:12}}},
            tooltip:{callbacks:{label:ctx=>[`${ctx.raw.label}`,`ROE: ${ctx.raw.x.toFixed(1)}%`,`GVI: ${ctx.raw.y.toFixed(2)}`]}},
          },
          scales:{
            x:{ title:{display:true,text:'ROE (%)',color:t.label}, ticks:{color:t.label,callback:v=>`${v}%`}, grid:{color:t.grid} },
            y:{ title:{display:true,text:'GVI 指標',color:t.label}, ticks:{color:t.label}, grid:{color:t.grid} },
          },
        },
      });
    },
    drawWinRate() {
      const t = this.theme();
      const pts = this.perfData.map(r => ({
        x: parseNum(r['勝率']),
        y: parseNum(r['盈虧比']),
        label: r['證券名稱']||r['證券代號'],
        cagr: parseNum(r['年化報酬率(CAGR)']),
      })).filter(p => p.x!==null && p.y!==null);
      const cf = p => p.x>=50&&p.y>=3?'rgba(34,197,94,0.85)':p.x<45&&p.y>=5?'rgba(139,92,246,0.85)':p.x>=60?'rgba(59,130,246,0.85)':'rgba(100,116,139,0.6)';
      const sf = p => Math.max(5, Math.min(20, (p.cagr||10)/5));
      this.destroyChart('scatterWinRef');
      new ChartJS(this.$refs.scatterWinRef, {
        type:'bubble',
        data:{ datasets:[{ label:'', data:pts.map(p=>({...p,r:sf(p)})), backgroundColor:pts.map(cf), borderColor:'transparent' }] },
        options:{
          responsive:true, maintainAspectRatio:false,
          plugins:{
            legend:{display:false},
            tooltip:{callbacks:{label:ctx=>[`${ctx.raw.label}`,`勝率: ${ctx.raw.x.toFixed(1)}%`,`盈虧比: ${ctx.raw.y.toFixed(2)}`,`CAGR: ${(ctx.raw.cagr||0).toFixed(1)}%`]}},
          },
          scales:{
            x:{ title:{display:true,text:'勝率 (%)',color:t.label}, ticks:{color:t.label,callback:v=>`${v}%`}, grid:{color:t.grid}, min:20 },
            y:{ title:{display:true,text:'盈虧比 (Profit Factor)',color:t.label}, ticks:{color:t.label}, grid:{color:t.grid}, min:0 },
          },
        },
      });
    },
  },
};
</script>

<style scoped>
.charts-dashboard { font-family:'Inter','Noto Sans TC',system-ui,sans-serif; padding:8px 0 24px; }
.charts-loading { display:flex; align-items:center; justify-content:center; gap:12px; padding:48px; color:var(--vp-c-text-2); font-size:14px; }
.spinner { width:24px; height:24px; border:3px solid var(--vp-c-divider); border-top-color:var(--vp-c-brand-1); border-radius:50%; animation:spin 0.8s linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }
.kpi-row { display:grid; grid-template-columns:repeat(auto-fit,minmax(155px,1fr)); gap:12px; margin-bottom:20px; }
.kpi-card { display:flex; align-items:center; gap:12px; padding:14px 16px; border-radius:12px; border:1px solid var(--vp-c-divider); background:var(--vp-c-bg-soft); transition:transform 0.15s,box-shadow 0.15s; }
.kpi-card:hover { transform:translateY(-2px); box-shadow:0 6px 20px rgba(0,0,0,0.1); }
.kpi-icon { font-size:15px; font-weight:700; width:34px; height:34px; border-radius:8px; display:flex; align-items:center; justify-content:center; flex-shrink:0; }
.kpi-card.buy   .kpi-icon { background:rgba(34,197,94,0.15);  color:#22c55e; }
.kpi-card.hold  .kpi-icon { background:rgba(59,130,246,0.15); color:#3b82f6; }
.kpi-card.sell  .kpi-icon { background:rgba(239,68,68,0.15);  color:#ef4444; }
.kpi-card.watch .kpi-icon { background:rgba(245,158,11,0.15); color:#f59e0b; }
.kpi-card.perf  .kpi-icon { background:rgba(139,92,246,0.15); color:#8b5cf6; }
.kpi-card.win   .kpi-icon { background:rgba(34,197,94,0.1);   color:#16a34a; }
.kpi-value { font-size:22px; font-weight:700; line-height:1.1; color:var(--vp-c-text-1); letter-spacing:-0.5px; }
.kpi-label { font-size:11px; color:var(--vp-c-text-2); margin-top:3px; line-height:1.3; }
.charts-grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px; }
@media(max-width:768px) { .charts-grid-2{grid-template-columns:1fr;} .kpi-row{grid-template-columns:1fr 1fr;} }
.chart-card { background:var(--vp-c-bg-soft); border:1px solid var(--vp-c-divider); border-radius:14px; padding:18px 20px 14px; height:420px; display:flex; flex-direction:column; gap:10px; transition:box-shadow 0.2s; }
.chart-card:hover { box-shadow:0 8px 28px rgba(0,0,0,0.08); }
.chart-card-full { margin-bottom:0; height:360px; }
.chart-title { font-size:13px; font-weight:600; color:var(--vp-c-text-1); flex-shrink:0; }
.chart-sub { font-size:11px; font-weight:400; color:var(--vp-c-text-2); margin-left:6px; }
canvas { flex:1; min-height:0; width:100% !important; }
</style>
