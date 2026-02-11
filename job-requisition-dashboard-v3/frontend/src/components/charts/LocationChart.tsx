import React from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import type { LocationStats } from '@/types/generated'

interface LocationChartProps {
  data: LocationStats[]
  isLoading?: boolean
}

const LocationChart: React.FC<LocationChartProps> = ({ data, isLoading }) => {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Top Locations</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-96 flex items-center justify-center">
            <p className="text-gray-500">Loading...</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  // Transform data for horizontal bar chart
  const chartData = data.map((item) => ({
    ...item,
    name: item.location.length > 30 ? item.location.substring(0, 30) + '...' : item.location,
  }))

  return (
    <Card>
      <CardHeader>
        <CardTitle>Top 10 Locations</CardTitle>
        <p className="text-sm text-gray-500">
          Locations with most open requisitions
        </p>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={400}>
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ top: 5, right: 30, left: 150, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis dataKey="name" type="category" width={140} />
            <Tooltip
              contentStyle={{
                backgroundColor: 'white',
                border: '1px solid #e5e7eb',
                borderRadius: '0.375rem',
              }}
              formatter={(value: number, name: string) => {
                if (name === 'count') return [value, 'Open Positions']
                if (name === 'avg_days') return [value.toFixed(1), 'Avg Days Open']
                return [value, name]
              }}
            />
            <Bar dataKey="count" fill="#3B82F6" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}

export default LocationChart
