import React from 'react'
import Layout from '@/components/layout/Layout'
import Header from '@/components/layout/Header'
import SummaryCard from '@/components/dashboard/SummaryCard'
import AgingChart from '@/components/charts/AgingChart'
import ReasonChart from '@/components/charts/ReasonChart'
import DepartmentChart from '@/components/charts/DepartmentChart'
import LocationChart from '@/components/charts/LocationChart'
import TrendChart from '@/components/charts/TrendChart'
import CriticalTable from '@/components/dashboard/CriticalTable'
import {
  useSummary,
  useAging,
  useDepartments,
  useLocations,
  useReasons,
  useTrends,
  useCritical,
} from '@/hooks/useAnalytics'
import { Briefcase, Building2, MapPin, Clock } from 'lucide-react'
import { formatNumber } from '@/lib/utils'

const Dashboard: React.FC = () => {
  const { data: summary, isLoading: summaryLoading } = useSummary()
  const { data: aging, isLoading: agingLoading } = useAging()
  const { data: departments, isLoading: departmentsLoading } = useDepartments()
  const { data: locations, isLoading: locationsLoading } = useLocations()
  const { data: reasons, isLoading: reasonsLoading } = useReasons()
  const { data: trends, isLoading: trendsLoading } = useTrends()
  const { data: critical, isLoading: criticalLoading } = useCritical()

  return (
    <Layout>
      <Header
        title="Job Requisition Dashboard"
        subtitle="Real-time analytics for open job requisitions"
        breadcrumbs={[
          { label: 'Home', href: '/' },
          { label: 'Dashboard' },
        ]}
      />

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <SummaryCard
          title="Total Open Requisitions"
          value={summaryLoading ? '...' : formatNumber(summary?.totalOpen || 0)}
          subtitle="Active job openings"
          icon={Briefcase}
          iconColor="text-primary"
        />
        <SummaryCard
          title="Departments Hiring"
          value={summaryLoading ? '...' : summary?.totalDepartments || 0}
          subtitle="With open positions"
          icon={Building2}
          iconColor="text-blue-600"
        />
        <SummaryCard
          title="Hiring Locations"
          value={summaryLoading ? '...' : summary?.totalLocations || 0}
          subtitle="Active locations"
          icon={MapPin}
          iconColor="text-green-600"
        />
        <SummaryCard
          title="Average Days Open"
          value={summaryLoading ? '...' : `${summary?.avgDaysOpen.toFixed(1) || 0}`}
          subtitle={`Max: ${summary?.maxDaysOpen || 0} days`}
          icon={Clock}
          iconColor="text-amber-600"
        />
      </div>

      {/* Charts Row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <AgingChart data={aging || []} isLoading={agingLoading} />
        <ReasonChart data={reasons || []} isLoading={reasonsLoading} />
      </div>

      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <DepartmentChart data={departments || []} isLoading={departmentsLoading} />
        <LocationChart data={locations || []} isLoading={locationsLoading} />
      </div>

      {/* Trend Chart */}
      <div className="mb-8">
        <TrendChart data={trends || []} isLoading={trendsLoading} />
      </div>

      {/* Critical Requisitions Table */}
      <CriticalTable data={critical || []} isLoading={criticalLoading} />
    </Layout>
  )
}

export default Dashboard
