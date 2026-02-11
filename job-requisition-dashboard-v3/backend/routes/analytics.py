"""
Analytics API Routes
Endpoints for dashboard analytics and statistics
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from models.analytics import (
    RequisitionSummary,
    AgingBucket,
    DepartmentStats,
    LocationStats,
    RequisitionReason,
    TrendDataPoint,
    CriticalRequisition
)
from services.analytics_service import AnalyticsService

router = APIRouter()


@router.get("/summary", response_model=RequisitionSummary)
async def get_summary(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get summary statistics for open job requisitions

    Returns:
        - Total open requisitions
        - Number of departments with openings
        - Number of locations with openings
        - Average days open
        - Maximum days open
        - Total positions
    """
    try:
        return AnalyticsService.get_summary(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")


@router.get("/aging", response_model=List[AgingBucket])
async def get_aging(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get aging analysis of open requisitions

    Returns aging buckets:
    - 0-14 Days
    - 15-30 Days
    - 31-60 Days
    - 61-90 Days
    - 90+ Days
    """
    try:
        return AnalyticsService.get_aging_analysis(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching aging data: {str(e)}")


@router.get("/departments", response_model=List[DepartmentStats])
async def get_departments(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get top 10 departments with open requisitions

    Returns:
        - Department name
        - Number of open requisitions
        - Average days open
    """
    try:
        return AnalyticsService.get_department_breakdown(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching department data: {str(e)}")


@router.get("/locations", response_model=List[LocationStats])
async def get_locations(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get top 10 locations with open requisitions

    Returns:
        - Location name
        - Number of open requisitions
        - Average days open
    """
    try:
        return AnalyticsService.get_location_breakdown(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching location data: {str(e)}")


@router.get("/reasons", response_model=List[RequisitionReason])
async def get_reasons(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get requisition reasons breakdown

    Returns:
        - Reason description
        - Count of requisitions
    """
    try:
        return AnalyticsService.get_requisition_reasons(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching reason data: {str(e)}")


@router.get("/trends", response_model=List[TrendDataPoint])
async def get_trends(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get monthly trend data for the last 12 months

    Returns:
        - Month (YYYY-MM)
        - Number of requisitions created
        - Number filled
        - Number still open
    """
    try:
        return AnalyticsService.get_monthly_trend(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching trend data: {str(e)}")


@router.get("/critical", response_model=List[CriticalRequisition])
async def get_critical(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get critical requisitions (90+ days open)

    Returns top 20 requisitions that have been open for 90+ days:
        - Requisition ID
        - Job title
        - Department
        - Location
        - Start date
        - Days open
    """
    try:
        return AnalyticsService.get_critical_requisitions(area, division, department)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching critical requisitions: {str(e)}")


@router.get("/refresh-timestamp")
async def get_refresh_timestamp():
    """
    Get the timestamp when the JobRequisition table was last refreshed

    Returns:
        - refresh_timestamp: ISO format timestamp of last data refresh
    """
    try:
        from services.database import execute_query
        results = execute_query("SELECT DISTINCT TOP 1 TableRefreshedDate FROM [Demo].[JobRequisition]")
        if results and results[0].get('TableRefreshedDate'):
            timestamp = results[0]['TableRefreshedDate']
            return {"refresh_timestamp": timestamp.isoformat()}
        return {"refresh_timestamp": None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching refresh timestamp: {str(e)}")
