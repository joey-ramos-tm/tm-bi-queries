"""
Pydantic models for requisition data
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Generic, TypeVar
from datetime import date


class RequisitionListItem(BaseModel):
    """Requisition list item for table view"""
    requisition: str = Field(..., description="Requisition ID")
    job_title: str = Field(..., description="Job title")
    department: str = Field(..., description="Department")
    location: str = Field(..., description="Location")
    status: str = Field(..., description="Requisition status")
    days_open: int = Field(..., description="Number of days open")
    start_date: Optional[str] = Field(None, description="Recruiting start date (YYYY-MM-DD)")
    target_hire_date: Optional[str] = Field(None, description="Target hire date (YYYY-MM-DD)")
    candidate_stage: Optional[str] = Field(None, description="Most recent candidate stage (highest ranked)")

    class Config:
        json_schema_extra = {
            "example": {
                "requisition": "REQ-2026-001",
                "job_title": "Sales Manager",
                "department": "Sales",
                "location": "Phoenix, AZ",
                "status": "Open",
                "days_open": 25,
                "start_date": "2026-01-10"
            }
        }


class RequisitionDetail(BaseModel):
    """Detailed requisition information"""
    requisition: str = Field(..., description="Requisition ID")
    job_title: str = Field(..., description="Job title")
    department: str = Field(..., description="Department")
    location: str = Field(..., description="Location")
    status: str = Field(..., description="Requisition status")
    recruiting_start_date: Optional[str] = Field(None, description="Recruiting start date")
    days_open: int = Field(..., description="Number of days open")
    reason: Optional[str] = Field(None, description="Requisition reason")
    hiring_manager: Optional[str] = Field(None, description="Hiring manager")
    job_profile: Optional[str] = Field(None, description="Job profile")
    supervisory_org: Optional[str] = Field(None, description="Supervisory organization")

    class Config:
        json_schema_extra = {
            "example": {
                "requisition": "REQ-2026-001",
                "job_title": "Sales Manager",
                "department": "Sales",
                "location": "Phoenix, AZ",
                "status": "Open",
                "recruiting_start_date": "2026-01-10",
                "days_open": 25,
                "reason": "New Position",
                "hiring_manager": "John Smith",
                "job_profile": "Sales Manager - Residential",
                "supervisory_org": "Sales - Southwest Region"
            }
        }


# Generic type for pagination
T = TypeVar('T')


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response"""
    data: List[T] = Field(..., description="List of items")
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page number (1-indexed)")
    page_size: int = Field(..., description="Number of items per page")
    total_pages: int = Field(..., description="Total number of pages")

    class Config:
        json_schema_extra = {
            "example": {
                "data": [
                    {
                        "requisition": "REQ-2026-001",
                        "job_title": "Sales Manager",
                        "department": "Sales",
                        "location": "Phoenix, AZ",
                        "status": "Open",
                        "days_open": 25,
                        "start_date": "2026-01-10"
                    }
                ],
                "total": 125,
                "page": 1,
                "page_size": 25,
                "total_pages": 5
            }
        }


class RequisitionFilters(BaseModel):
    """Query parameters for filtering requisitions"""
    status: Optional[str] = Field(None, description="Filter by status (e.g., 'Open', 'Filled')")
    department: Optional[str] = Field(None, description="Filter by department")
    location: Optional[str] = Field(None, description="Filter by location")
    min_days_open: Optional[int] = Field(None, description="Minimum days open")
    max_days_open: Optional[int] = Field(None, description="Maximum days open")
    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(25, ge=1, le=100, description="Items per page (1-100)")
    sort_by: Optional[str] = Field("days_open", description="Field to sort by")
    sort_order: Optional[str] = Field("desc", description="Sort order: 'asc' or 'desc'")

    class Config:
        json_schema_extra = {
            "example": {
                "status": "Open",
                "department": "Sales",
                "page": 1,
                "page_size": 25,
                "sort_by": "days_open",
                "sort_order": "desc"
            }
        }


class ExportRequest(BaseModel):
    """Request model for exporting data to CSV"""
    filters: Optional[RequisitionFilters] = Field(None, description="Filters to apply before export")

    class Config:
        json_schema_extra = {
            "example": {
                "filters": {
                    "status": "Open",
                    "min_days_open": 30
                }
            }
        }
