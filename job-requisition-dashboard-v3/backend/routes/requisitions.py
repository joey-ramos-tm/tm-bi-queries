"""
Requisition API Routes
Endpoints for requisition data retrieval
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from models.requisition import (
    RequisitionListItem,
    RequisitionDetail,
    PaginatedResponse,
    RequisitionFilters
)
from services.requisition_service import RequisitionService

router = APIRouter()


@router.get("", response_model=PaginatedResponse[RequisitionListItem])
async def get_requisitions(
    status: Optional[str] = Query(None, description="Filter by status"),
    department: Optional[str] = Query(None, description="Filter by department"),
    location: Optional[str] = Query(None, description="Filter by location"),
    min_days_open: Optional[int] = Query(None, description="Minimum days open"),
    max_days_open: Optional[int] = Query(None, description="Maximum days open"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(25, ge=1, le=100, description="Items per page (1-100)"),
    sort_by: str = Query("days_open", description="Field to sort by"),
    sort_order: str = Query("desc", description="Sort order: 'asc' or 'desc'")
):
    """
    Get paginated list of requisitions with optional filters

    Query Parameters:
        - status: Filter by status (e.g., 'Open', 'Filled')
        - department: Filter by department name
        - location: Filter by location name
        - min_days_open: Minimum days the requisition has been open
        - max_days_open: Maximum days the requisition has been open
        - page: Page number (default: 1)
        - page_size: Number of items per page (default: 25, max: 100)
        - sort_by: Field to sort by (default: 'days_open')
        - sort_order: Sort order 'asc' or 'desc' (default: 'desc')

    Returns:
        - data: List of requisitions
        - total: Total number of items
        - page: Current page number
        - page_size: Items per page
        - total_pages: Total number of pages
    """
    try:
        filters = RequisitionFilters(
            status=status,
            department=department,
            location=location,
            min_days_open=min_days_open,
            max_days_open=max_days_open,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_order=sort_order
        )

        return RequisitionService.get_requisitions(filters)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching requisitions: {str(e)}"
        )


@router.get("/{requisition_id}", response_model=RequisitionDetail)
async def get_requisition_by_id(requisition_id: str):
    """
    Get detailed information for a specific requisition

    Path Parameters:
        - requisition_id: The requisition ID to retrieve

    Returns:
        - Full requisition details including job profile, reason, etc.

    Raises:
        - 404: If requisition not found
    """
    try:
        requisition = RequisitionService.get_requisition_by_id(requisition_id)

        if not requisition:
            raise HTTPException(
                status_code=404,
                detail=f"Requisition {requisition_id} not found"
            )

        return requisition

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching requisition: {str(e)}"
        )
