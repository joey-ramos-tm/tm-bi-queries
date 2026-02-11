/**
 * React Query hooks for analytics data
 */
import { useQuery } from '@tanstack/react-query'
import { analyticsApi } from '@/lib/api'
import type {
  RequisitionSummary,
  AgingBucket,
  DepartmentStats,
  LocationStats,
  TrendDataPoint,
  RequisitionDetail
} from '@/types/generated'

interface RequisitionReason {
  reason: string
  count: number
}

/**
 * Hook to fetch summary statistics
 */
export function useSummary() {
  return useQuery<RequisitionSummary>({
    queryKey: ['analytics', 'summary'],
    queryFn: async () => {
      const response = await analyticsApi.getSummary()
      return response.data
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

/**
 * Hook to fetch aging analysis
 */
export function useAging() {
  return useQuery<AgingBucket[]>({
    queryKey: ['analytics', 'aging'],
    queryFn: async () => {
      const response = await analyticsApi.getAging()
      return response.data
    },
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Hook to fetch department breakdown
 */
export function useDepartments() {
  return useQuery<DepartmentStats[]>({
    queryKey: ['analytics', 'departments'],
    queryFn: async () => {
      const response = await analyticsApi.getDepartments()
      return response.data
    },
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Hook to fetch location breakdown
 */
export function useLocations() {
  return useQuery<LocationStats[]>({
    queryKey: ['analytics', 'locations'],
    queryFn: async () => {
      const response = await analyticsApi.getLocations()
      return response.data
    },
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Hook to fetch requisition reasons
 */
export function useReasons() {
  return useQuery<RequisitionReason[]>({
    queryKey: ['analytics', 'reasons'],
    queryFn: async () => {
      const response = await analyticsApi.getReasons()
      return response.data
    },
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Hook to fetch monthly trends
 */
export function useTrends() {
  return useQuery<TrendDataPoint[]>({
    queryKey: ['analytics', 'trends'],
    queryFn: async () => {
      const response = await analyticsApi.getTrends()
      return response.data
    },
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Hook to fetch critical requisitions (90+ days)
 */
export function useCritical() {
  return useQuery<RequisitionDetail[]>({
    queryKey: ['analytics', 'critical'],
    queryFn: async () => {
      const response = await analyticsApi.getCritical()
      return response.data
    },
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Hook to fetch all analytics data at once
 */
export function useAllAnalytics() {
  const summary = useSummary()
  const aging = useAging()
  const departments = useDepartments()
  const locations = useLocations()
  const reasons = useReasons()
  const trends = useTrends()
  const critical = useCritical()

  return {
    summary,
    aging,
    departments,
    locations,
    reasons,
    trends,
    critical,
    isLoading:
      summary.isLoading ||
      aging.isLoading ||
      departments.isLoading ||
      locations.isLoading ||
      reasons.isLoading ||
      trends.isLoading ||
      critical.isLoading,
    isError:
      summary.isError ||
      aging.isError ||
      departments.isError ||
      locations.isError ||
      reasons.isError ||
      trends.isError ||
      critical.isError,
  }
}
