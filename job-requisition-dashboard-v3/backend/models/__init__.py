"""Pydantic Models package"""

from .analytics import (
    RequisitionSummary,
    AgingBucket,
    DepartmentStats,
    LocationStats,
    RequisitionReason,
    TrendDataPoint,
    CriticalRequisition,
    AnalyticsResponse
)

from .requisition import (
    RequisitionListItem,
    RequisitionDetail,
    PaginatedResponse,
    RequisitionFilters,
    ExportRequest
)

from .filled_analytics import (
    FilledSummary,
    MonthlyFilledCount,
    DivisionFilledStats,
    DepartmentFilledStats,
    AreaFilledStats,
    FilledRequisition,
    FilledAnalyticsResponse
)

__all__ = [
    # Analytics models
    'RequisitionSummary',
    'AgingBucket',
    'DepartmentStats',
    'LocationStats',
    'RequisitionReason',
    'TrendDataPoint',
    'CriticalRequisition',
    'AnalyticsResponse',
    # Requisition models
    'RequisitionListItem',
    'RequisitionDetail',
    'PaginatedResponse',
    'RequisitionFilters',
    'ExportRequest',
    # Filled Analytics models
    'FilledSummary',
    'MonthlyFilledCount',
    'DivisionFilledStats',
    'DepartmentFilledStats',
    'AreaFilledStats',
    'FilledRequisition',
    'FilledAnalyticsResponse'
]
