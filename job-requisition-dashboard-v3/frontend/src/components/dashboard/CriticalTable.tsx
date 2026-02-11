import React from 'react'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import type { RequisitionDetail } from '@/types/generated'

interface CriticalTableProps {
  data: RequisitionDetail[]
  isLoading?: boolean
}

const CriticalTable: React.FC<CriticalTableProps> = ({ data, isLoading }) => {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Critical Requisitions (90+ Days)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-64 flex items-center justify-center">
            <p className="text-gray-500">Loading...</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  if (data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Critical Requisitions (90+ Days)</CardTitle>
          <p className="text-sm text-gray-500">
            Requisitions open for 90 or more days
          </p>
        </CardHeader>
        <CardContent>
          <div className="h-32 flex items-center justify-center">
            <p className="text-gray-500">No critical requisitions found</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Critical Requisitions (90+ Days)</CardTitle>
        <p className="text-sm text-gray-500">
          Top {data.length} requisitions that need immediate attention
        </p>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Requisition ID</TableHead>
              <TableHead>Job Title</TableHead>
              <TableHead>Department</TableHead>
              <TableHead>Location</TableHead>
              <TableHead>Start Date</TableHead>
              <TableHead className="text-right">Days Open</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.map((req) => (
              <TableRow key={req.requisitionId}>
                <TableCell className="font-medium">
                  {req.requisitionId}
                </TableCell>
                <TableCell>{req.jobTitle}</TableCell>
                <TableCell>{req.department}</TableCell>
                <TableCell>{req.location}</TableCell>
                <TableCell>{req.createdDate}</TableCell>
                <TableCell className="text-right">
                  <Badge variant="destructive">
                    {req.daysOpen} days
                  </Badge>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  )
}

export default CriticalTable
