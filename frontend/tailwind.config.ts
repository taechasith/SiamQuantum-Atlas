import type { Config } from 'tailwindcss'

export default {
  content: [
    './index.html',
    './src/**/*.{vue,js,ts,jsx,tsx}',
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        sq: {
          bg: '#080f1b',
          sidebar: '#080f1b',
          surface: '#131B2E',
          'surface-2': '#192338',
          'surface-hover': '#202D46',
          primary: '#329DD2',
          secondary: '#10B981',
          tertiary: '#329DD2',
          warning: '#F59E0B',
          danger: '#F87171',
        },
      },
      fontFamily: {
        sans: ['"IBM Plex Sans Thai"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'monospace'],
      },
    },
  },
  plugins: [],
} satisfies Config
