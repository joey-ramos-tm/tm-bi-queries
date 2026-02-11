import React from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type { AgingBucket } from '@/types/generated'

interface AgingChartProps {
  data: AgingBucket[]
  isLoading?: boolean
}

// Color mapping for urgency levels
const getColorForBucket = (category: string): string => {
  const colorMap: Record<string, string> = {
    '0-14 Days': '#10B981', // green
    '15-30 Days': '#3B82F6', // blue
    '31-60 Days': '#F59E0B', // amber
    '61-90 Days': '#EF4444', // red
    '90+ Days': '#991B1B', // dark red
  }
  return colorMap[category] || '#6B7280'
}

const AgingChart: React.FC<AgingChartProps> = ({ data, isLoading }) => {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Aging Analysis</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80 flex items-center justify-center">
            <p className="text-gray-500">Loading...</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Aging Analysis</CardTitle>
        <p className="text-sm text-gray-500">
          Distribution of requisitions by days open
        </p>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="category" />
            <YAxis />
            <Tooltip
              contentStyle={{
                backgroundColor: 'white',
                border: '1px solid #e5e7eb',
                borderRadius: '0.375rem',
              }}
            />
            <Bar dataKey="count" radius={[8, 8, 0, 0]}>
              {data.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={getColorForBucket(entry.category)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}

export default AgingChart
