import { defineConfig } from 'vitepress'

const isProduction = process.env.NODE_ENV === 'production';
const BASE_URL = isProduction ? '/StockVitePress/' : '';

// https://vitepress.dev/reference/site-config
export default defineConfig({
  base: `${BASE_URL}`,
  title: "Stock VitePress",
  description: "use vitepress display stock info",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
      { text: '主力籌碼選股雷達', link: '/Stock_GVI_SmartMoney' },
      { text: 'GVI 回測儀表板', link: '/Stock_GVI_Backtest' }
    ],

    sidebar: [
      {
        text: '選股與回測分析',
        items: [
          { text: '⚡ 主力籌碼 × GVI 雷達', link: '/Stock_GVI_SmartMoney' },
          { text: '📈 GVI 回測與買賣時機', link: '/Stock_GVI_Backtest' },
          { text: '📑 GVI + 三因子原始清單', link: '/Stock_GVI_ThreeFactor' },
          { text: '📊 篩選基本資料', link: '/basic_stock_info' },
          { text: '🏛️ 三大法人', link: '/institutionI_investors' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/vuejs/vitepress' }
    ]
  }
})
