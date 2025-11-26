'use client'

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import { formatCurrency } from '@/lib/utils'
import type { ForecastTrendPoint } from '@/types'

interface ForecastTrendChartProps {
  data: ForecastTrendPoint[]
  currentWeek?: number
  className?: string
}

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload) return null

  return (
    <div className="bg-bg-elevated border border-border-default rounded-lg p-3 shadow-lg">
      <p className="text-sm font-medium text-text-primary mb-2">{label}</p>
      {payload.map((entry: any, index: number) => (
        <div key={index} className="flex items-center gap-2 text-sm">
          <div
            className="w-2 h-2 rounded-full"
            style={{ backgroundColor: entry.color }}
          />
          <span className="text-text-secondary">{entry.name}:</span>
          <span className="font-mono text-text-primary">
            {formatCurrency(entry.value)}
          </span>
        </div>
      ))}
    </div>
  )
}

export function ForecastTrendChart({
  data,
  currentWeek = 7,
  className,
}: ForecastTrendChartProps) {
  return (
    <div className={className}>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart
          data={data}
          margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
        >
          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#27272a"
            vertical={false}
          />
          <XAxis
            dataKey="week"
            stroke="#71717a"
            fontSize={12}
            tickLine={false}
            axisLine={false}
          />
          <YAxis
            stroke="#71717a"
            fontSize={12}
            tickLine={false}
            axisLine={false}
            tickFormatter={(value) => `$${(value / 1000000).toFixed(0)}M`}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend
            verticalAlign="top"
            align="right"
            iconType="line"
            wrapperStyle={{ paddingBottom: '20px' }}
          />

          {/* Target line - dashed white */}
          <Line
            type="monotone"
            dataKey="target"
            name="Target"
            stroke="#ffffff"
            strokeWidth={2}
            strokeDasharray="5 5"
            dot={false}
          />

          {/* Commit line - green */}
          <Line
            type="monotone"
            dataKey="commit"
            name="Commit"
            stroke="#22c55e"
            strokeWidth={2}
            dot={false}
          />

          {/* AI Forecast line - blue */}
          <Line
            type="monotone"
            dataKey="forecast"
            name="AI Forecast"
            stroke="#3b82f6"
            strokeWidth={2}
            dot={false}
          />

          {/* Closed Won area - amber/filled */}
          <Line
            type="monotone"
            dataKey="closed"
            name="Closed Won"
            stroke="#f59e0b"
            strokeWidth={2}
            fill="#f59e0b"
            fillOpacity={0.1}
            dot={false}
          />

          {/* Current week marker */}
          <ReferenceLine
            x={`W${currentWeek}`}
            stroke="#52525b"
            strokeDasharray="3 3"
            label={{
              value: 'Today',
              position: 'top',
              fill: '#71717a',
              fontSize: 11,
            }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
