import React, { useState } from 'react'
import Layout from '@/components/layout/Layout'
import Header from '@/components/layout/Header'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Select } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { useRequisitions } from '@/hooks/useRequisitions'
import { useExportToCsv } from '@/hooks/useExport'
import { Download, ChevronLeft, ChevronRight, ArrowUpDown } from 'lucide-react'

const Requisitions: React.FC = () => {
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(25)
  const [sortBy, setSortBy] = useState('days_open')
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc')
  const [statusFilter, setStatusFilter] = useState('')
  const [departmentFilter, setDepartmentFilter] = useState('')
  const [locationFilter, setLocationFilter] = useState('')

  const { data, isLoading, error } = useRequisitions({
    page,
    pageSize,
    sortBy,
    sortOrder,
    status: statusFilter || undefined,
    department: departmentFilter || undefined,
    location: locationFilter || undefined,
  })

  const exportMutation = useExportToCsv()

  const handleExport = () => {
    exportMutation.mutate({
      status: statusFilter || undefined,
      department: departmentFilter || undefined,
      location: locationFilter || undefined,
    })
  }

  const handleSort = (field: string) => {
    if (sortBy === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(field)
      setSortOrder('desc')
    }
    setPage(1)
  }

  const handlePageSizeChange = (newSize: number) => {
    setPageSize(newSize)
    setPage(1)
  }

  const handlePreviousPage = () => {
    if (page > 1) setPage(page - 1)
  }

  const handleNextPage = () => {
    if (data && page < data.totalPages) setPage(page + 1)
  }

  return (
    <Layout>
      <Header
        title="Requisitions Table"
        subtitle="View and filter all open job requisitions"
        breadcrumbs={[
          { label: 'Home', href: '/' },
          { label: 'Requisitions' },
        ]}
      />

      {/* Filters and Actions */}
      <Card className="mb-6">
        <CardContent className="pt-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
            {/* Status Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Status
              </label>
              <Select
                value={statusFilter}
                onChange={(e) => {
                  setStatusFilter(e.target.value)
                  setPage(1)
                }}
              >
                <option value="">All Statuses</option>
                <option value="Open">Open</option>
                <option value="Filled">Filled</option>
              </Select>
            </div>

            {/* Department Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Department
              </label>
              <input
                type="text"
                placeholder="Filter by department..."
                className="w-full h-10 px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent"
                value={departmentFilter}
                onChange={(e) => {
                  setDepartmentFilter(e.target.value)
                  setPage(1)
                }}
              />
            </div>

            {/* Location Filter */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Location
              </label>
              <input
                type="text"
                placeholder="Filter by location..."
                className="w-full h-10 px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent"
                value={locationFilter}
                onChange={(e) => {
                  setLocationFilter(e.target.value)
                  setPage(1)
                }}
              />
            </div>

            {/* Page Size */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Per Page
              </label>
              <Select
                value={pageSize.toString()}
                onChange={(e) => handlePageSizeChange(Number(e.target.value))}
              >
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
              </Select>
            </div>
          </div>

          {/* Actions */}
          <div className="flex justify-between items-center">
            <div className="text-sm text-gray-600">
              {data && (
                <>
                  Showing {((page - 1) * pageSize) + 1} to{' '}
                  {Math.min(page * pageSize, data.total)} of {data.total} results
                </>
              )}
            </div>
            <Button
              onClick={handleExport}
              disabled={exportMutation.isLoading}
              variant="outline"
              size="sm"
              className="flex items-center space-x-2"
            >
              <Download className="h-4 w-4" />
              <span>
                {exportMutation.isLoading ? 'Exporting...' : 'Export to CSV'}
              </span>
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Table */}
      <Card>
        <CardContent className="pt-6">
          {isLoading ? (
            <div className="h-64 flex items-center justify-center">
              <p className="text-gray-500">Loading requisitions...</p>
            </div>
          ) : error ? (
            <div className="h-64 flex items-center justify-center">
              <p className="text-red-500">Error loading requisitions</p>
            </div>
          ) : data && data.data.length > 0 ? (
            <>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <button
                        onClick={() => handleSort('requisition')}
                        className="flex items-center space-x-1 hover:text-primary"
                      >
                        <span>Requisition ID</span>
                        <ArrowUpDown className="h-4 w-4" />
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        onClick={() => handleSort('job_title')}
                        className="flex items-center space-x-1 hover:text-primary"
                      >
                        <span>Job Title</span>
                        <ArrowUpDown className="h-4 w-4" />
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        onClick={() => handleSort('department')}
                        className="flex items-center space-x-1 hover:text-primary"
                      >
                        <span>Department</span>
                        <ArrowUpDown className="h-4 w-4" />
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        onClick={() => handleSort('location')}
                        className="flex items-center space-x-1 hover:text-primary"
                      >
                        <span>Location</span>
                        <ArrowUpDown className="h-4 w-4" />
                      </button>
                    </TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>
                      <button
                        onClick={() => handleSort('days_open')}
                        className="flex items-center space-x-1 hover:text-primary"
                      >
                        <span>Days Open</span>
                        <ArrowUpDown className="h-4 w-4" />
                      </button>
                    </TableHead>
                    <TableHead>Start Date</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.data.map((req) => (
                    <TableRow key={req.requisition}>
                      <TableCell className="font-medium">
                        {req.requisition}
                      </TableCell>
                      <TableCell>{req.jobTitle}</TableCell>
                      <TableCell>{req.department}</TableCell>
                      <TableCell>{req.location}</TableCell>
                      <TableCell>
                        <Badge
                          variant={req.status.includes('Open') ? 'default' : 'secondary'}
                        >
                          {req.status}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant={
                            req.daysOpen > 90
                              ? 'destructive'
                              : req.daysOpen > 60
                              ? 'warning'
                              : 'secondary'
                          }
                        >
                          {req.daysOpen} days
                        </Badge>
                      </TableCell>
                      <TableCell>
                        {req.startDate || 'N/A'}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>

              {/* Pagination */}
              <div className="flex items-center justify-between mt-6">
                <div className="text-sm text-gray-600">
                  Page {page} of {data.totalPages}
                </div>
                <div className="flex space-x-2">
                  <Button
                    onClick={handlePreviousPage}
                    disabled={page === 1}
                    variant="outline"
                    size="sm"
                  >
                    <ChevronLeft className="h-4 w-4 mr-1" />
                    Previous
                  </Button>
                  <Button
                    onClick={handleNextPage}
                    disabled={page === data.totalPages}
                    variant="outline"
                    size="sm"
                  >
                    Next
                    <ChevronRight className="h-4 w-4 ml-1" />
                  </Button>
                </div>
              </div>
            </>
          ) : (
            <div className="h-64 flex items-center justify-center">
              <p className="text-gray-500">No requisitions found</p>
            </div>
          )}
        </CardContent>
      </Card>
    </Layout>
  )
}

export default Requisitions
