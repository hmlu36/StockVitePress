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
      { text: 'Examples', link: '/markdown-examples' }
    ],

    sidebar: [
      {
        text: 'Examples',
        items: [
          { text: 'Markdown Examples', link: '/markdown-examples' },
          { text: '三大法人', link: '/institutionI_investors' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/vuejs/vitepress' }
    ]
  }
})
