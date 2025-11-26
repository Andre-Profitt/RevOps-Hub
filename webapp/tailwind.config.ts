import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        // Background colors
        'bg-primary': '#09090b',
        'bg-secondary': '#18181b',
        'bg-elevated': '#27272a',
        'bg-interactive': '#3f3f46',

        // Text colors
        'text-primary': '#fafafa',
        'text-secondary': '#a1a1aa',
        'text-muted': '#71717a',
        'text-disabled': '#52525b',

        // Border colors
        'border-subtle': '#27272a',
        'border-default': '#3f3f46',
        'border-hover': '#52525b',

        // Status colors
        'status-healthy': '#22c55e',
        'status-monitor': '#eab308',
        'status-at-risk': '#f97316',
        'status-critical': '#ef4444',

        // Accent colors
        'accent-blue': '#3b82f6',
        'accent-purple': '#8b5cf6',
        'accent-cyan': '#06b6d4',

        // Chart colors
        'chart-1': '#3b82f6',
        'chart-2': '#22c55e',
        'chart-3': '#f59e0b',
        'chart-4': '#ef4444',
        'chart-5': '#8b5cf6',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'SF Mono', 'monospace'],
      },
      fontSize: {
        'display': ['2rem', { lineHeight: '2.5rem', fontWeight: '600' }],
        'metric-hero': ['3rem', { lineHeight: '3.5rem', fontWeight: '600' }],
        'metric-large': ['2rem', { lineHeight: '2.5rem', fontWeight: '600' }],
        'metric-medium': ['1.5rem', { lineHeight: '2rem', fontWeight: '500' }],
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'shimmer': 'shimmer 1.5s infinite',
        'count-up': 'countUp 0.4s ease-out',
      },
      keyframes: {
        shimmer: {
          '0%': { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition: '200% 0' },
        },
        countUp: {
          '0%': { opacity: '0', transform: 'translateY(10px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
      },
    },
  },
  plugins: [],
}

export default config
