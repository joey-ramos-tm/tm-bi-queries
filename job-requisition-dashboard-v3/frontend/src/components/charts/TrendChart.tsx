import React from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type { TrendDataPoint } from '@/types/generated'

interface TrendChartProps {
  data: TrendDataPoint[]
  isLoading?: boolean
}

const TrendChart: React.FC<TrendChartProps> = ({ data, isLoading }) => {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Monthly Trends</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80 flex items-center justify-center">
            <p className="text-gray-500">Loading...</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  // Transform month format for better display
  const chartData = data.map((item) => ({
    ...item,
    displayMonth: item.month ? new Date(item.month + '-01').toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
    }) : '',
  }))

  return (
    <Card>
      <CardHeader>
        <CardTitle>12-Month Trends</CardTitle>
        <p className="text-sm text-gray-500">
          Requisitions created, filled, and still open over time
        </p>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="displayMonth" />
            <YAxis />
            <Tooltip
              contentStyle={{
                backgroundColor: 'white',
                border: '1px solid #e5e7eb',
                borderRadius: '0.375rem',
              }}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="created"
              stroke="#3B82F6"
              strokeWidth={2}
              name="Created"
              dot={{ r: 4 }}
              activeDot={{ r: 6 }}
            />
            <Line
              type="monotone"
              dataKey="filled"
              stroke="#10B981"
              strokeWidth={2}
              name="Filled"
              dot={{ r: 4 }}
              activeDot={{ r: 6 }}
            />
            <Line
              type="monotone"
              dataKey="still_open"
              stroke="#EF4444"
              strokeWidth={2}
              name="Still Open"
              dot={{ r: 4 }}
              activeDot={{ r: 6 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}

export default TrendChart
