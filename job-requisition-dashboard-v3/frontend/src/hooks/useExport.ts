/**
 * React Query hooks for data export
 */
import { useMutation } from '@tanstack/react-query'
import { exportsApi, type RequisitionFilters } from '@/lib/api'

/**
 * Hook to export requisitions to CSV
 */
export function useExportToCsv() {
  return useMutation({
    mutationFn: async (filters?: RequisitionFilters) => {
      const response = await exportsApi.exportToCsv(filters)

      // Create blob from response
      const blob = new Blob([response.data], { type: 'text/csv' })

      // Create download link
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url

      // Extract filename from headers or use default
      const contentDisposition = response.headers['content-disposition']
      let filename = 'requisitions_export.csv'

      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="?(.+)"?/)
        if (filenameMatch) {
          filename = filenameMatch[1]
        }
      }

      link.download = filename

      // Trigger download
      document.body.appendChild(link)
      link.click()

      // Cleanup
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)

      return { success: true, filename }
    },
  })
}
