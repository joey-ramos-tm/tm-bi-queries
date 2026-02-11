import React from 'react'
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
  Legend,
} from 'recharts'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

interface ReasonData {
  reason: string
  count: number
}

interface ReasonChartProps {
  data: ReasonData[]
  isLoading?: boolean
}

// Color palette for the donut chart
const COLORS = [
  '#A6192E', // Taylor Morrison burgundy
  '#3B82F6', // blue
  '#10B981', // green
  '#F59E0B', // amber
  '#8B5CF6', // purple
  '#EC4899', // pink
  '#6B7280', // gray
]

const ReasonChart: React.FC<ReasonChartProps> = ({ data, isLoading }) => {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Requisition Reasons</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80 flex items-center justify-center">
            <p className="text-gray-500">Loading...</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  // Limit to top 6 reasons
  const chartData = data.slice(0, 6)

  return (
    <Card>
      <CardHeader>
        <CardTitle>Requisition Reasons</CardTitle>
        <p className="text-sm text-gray-500">
          Distribution of reasons for open requisitions
        </p>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              innerRadius={60}
              outerRadius={100}
              fill="#8884d8"
              paddingAngle={2}
              dataKey="count"
              label={(entry) => `${entry.reason}: ${entry.count}`}
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{
                backgroundColor: 'white',
                border: '1px solid #e5e7eb',
                borderRadius: '0.375rem',
              }}
            />
            <Legend verticalAlign="bottom" height={36} />
          </PieChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}

export default ReasonChart
