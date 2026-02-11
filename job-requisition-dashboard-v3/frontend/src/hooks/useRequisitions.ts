/**
 * React Query hooks for requisition data
 */
import { useQuery } from '@tanstack/react-query'
import { requisitionsApi, type RequisitionFilters } from '@/lib/api'
import type { PaginatedResponse, RequisitionListItem, RequisitionDetail } from '@/types/generated'

/**
 * Hook to fetch paginated requisitions list
 */
export function useRequisitions(filters?: RequisitionFilters) {
  return useQuery<PaginatedResponse<RequisitionListItem>>({
    queryKey: ['requisitions', filters],
    queryFn: async () => {
      const response = await requisitionsApi.getList(filters)
      return response.data
    },
    staleTime: 2 * 60 * 1000, // 2 minutes
    keepPreviousData: true, // Keep previous data while fetching new page
  })
}

/**
 * Hook to fetch a single requisition by ID
 */
export function useRequisition(id: string) {
  return useQuery<RequisitionDetail>({
    queryKey: ['requisition', id],
    queryFn: async () => {
      const response = await requisitionsApi.getById(id)
      return response.data
    },
    enabled: !!id, // Only run if ID is provided
    staleTime: 5 * 60 * 1000,
  })
}
