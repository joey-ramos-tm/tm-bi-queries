"""
Filled Analytics API Routes
Endpoints for filled requisitions analytics
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from models.filled_analytics import (
    FilledSummary,
    MonthlyFilledCount,
    DivisionFilledStats,
    DepartmentFilledStats,
    AreaFilledStats,
    FilledRequisition
)
from services.filled_analytics_service import FilledAnalyticsService

router = APIRouter()


@router.get("/summary", response_model=FilledSummary)
async def get_filled_summary(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get summary statistics for filled requisitions in the last 13 months

    Returns:
        - Total filled requisitions
        - Average time to fill
        - Median time to fill
        - Fastest and slowest fill times
        - Number of divisions and departments
    """
    try:
        return FilledAnalyticsService.get_filled_summary(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching filled summary: {str(e)}")


@router.get("/monthly", response_model=List[MonthlyFilledCount])
async def get_monthly_trend(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get monthly filled requisitions trend for the last 13 months

    Returns:
        - Month (YYYY-MM)
        - Number of requisitions filled
        - Average time to fill for the month
    """
    try:
        return FilledAnalyticsService.get_monthly_trend(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching monthly trend: {str(e)}")


@router.get("/by-division", response_model=List[DivisionFilledStats])
async def get_division_breakdown(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get top 10 divisions by filled requisitions count

    Returns:
        - Division name
        - Number of filled requisitions
        - Average time to fill
        - Current open requisitions count
    """
    try:
        return FilledAnalyticsService.get_division_breakdown(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching division breakdown: {str(e)}")


@router.get("/by-department", response_model=List[DepartmentFilledStats])
async def get_department_breakdown(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get top 10 departments by filled requisitions count

    Returns:
        - Department name (Job Family Group)
        - Number of filled requisitions
        - Average time to fill
        - Current open requisitions count
    """
    try:
        return FilledAnalyticsService.get_department_breakdown(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching department breakdown: {str(e)}")


@router.get("/by-area", response_model=List[AreaFilledStats])
async def get_area_breakdown(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get top 10 areas by filled requisitions count

    Returns:
        - Area name
        - Number of filled requisitions
        - Average time to fill
    """
    try:
        return FilledAnalyticsService.get_area_breakdown(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching area breakdown: {str(e)}")


@router.get("/recent", response_model=List[FilledRequisition])
async def get_recent_filled(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department"),
    limit: int = Query(20, description="Maximum number of results", ge=1, le=100)
):
    """
    Get recently filled requisitions (last 13 months)

    Returns list of filled requisitions with:
        - Requisition ID
        - Job title
        - Department
        - Division
        - Area
        - Location
        - Recruiting start date
        - Filled date
        - Days to fill
    """
    try:
        return FilledAnalyticsService.get_recent_filled(area, division, department, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching recent filled requisitions: {str(e)}")
