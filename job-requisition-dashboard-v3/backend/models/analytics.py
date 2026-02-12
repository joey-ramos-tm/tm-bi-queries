"""
Pydantic models for analytics and dashboard data
"""
from pydantic import BaseModel, Field
from typing import List, Optional


class RequisitionSummary(BaseModel):
    """Summary statistics for open job requisitions"""
    total_open: int = Field(..., description="Total number of open requisitions")
    departments: int = Field(..., description="Number of departments with open positions")
    locations: int = Field(..., description="Number of locations with open positions")
    avg_days_open: float = Field(..., description="Average days requisitions have been open")
    max_days_open: int = Field(..., description="Maximum days a requisition has been open")
    total_positions: int = Field(..., description="Total number of positions")
    filled_last_90_days: int = Field(..., description="Number of requisitions filled in the last 90 days")

    class Config:
        json_schema_extra = {
            "example": {
                "total_open": 125,
                "departments": 15,
                "locations": 20,
                "avg_days_open": 45.3,
                "max_days_open": 180,
                "total_positions": 125
            }
        }


class AgingBucket(BaseModel):
    """Aging analysis bucket"""
    category: str = Field(..., description="Age category (e.g., '0-14 Days', '15-30 Days')")
    count: int = Field(..., description="Number of requisitions in this age bucket")

    class Config:
        json_schema_extra = {
            "example": {
                "category": "15-30 Days",
                "count": 42
            }
        }


class DepartmentStats(BaseModel):
    """Department statistics"""
    department: str = Field(..., description="Department name")
    count: int = Field(..., description="Number of open requisitions")
    avg_days: float = Field(..., description="Average days open for this department")
    filled_last_90_days: int = Field(..., description="Number filled in last 90 days")

    class Config:
        json_schema_extra = {
            "example": {
                "department": "Sales",
                "count": 25,
                "avg_days": 38.5
            }
        }


class LocationStats(BaseModel):
    """Location statistics"""
    location: str = Field(..., description="Location name")
    count: int = Field(..., description="Number of open requisitions")
    avg_days: float = Field(..., description="Average days open for this location")
    filled_last_90_days: int = Field(..., description="Number filled in last 90 days")

    class Config:
        json_schema_extra = {
            "example": {
                "location": "Phoenix, AZ",
                "count": 18,
                "avg_days": 42.1
            }
        }


class RequisitionReason(BaseModel):
    """Requisition reason statistics"""
    reason: str = Field(..., description="Reason for requisition")
    count: int = Field(..., description="Number of requisitions with this reason")

    class Config:
        json_schema_extra = {
            "example": {
                "reason": "New Position",
                "count": 45
            }
        }


class TrendDataPoint(BaseModel):
    """Monthly trend data point"""
    month: str = Field(..., description="Month in YYYY-MM format")
    created: int = Field(..., description="Number of requisitions created")
    filled: int = Field(..., description="Number of requisitions filled")
    still_open: int = Field(..., description="Number of requisitions still open")

    class Config:
        json_schema_extra = {
            "example": {
                "month": "2026-01",
                "created": 35,
                "filled": 22,
                "still_open": 13
            }
        }


class CriticalRequisition(BaseModel):
    """Critical requisition (90+ days open)"""
    requisition: str = Field(..., description="Requisition ID")
    job_title: str = Field(..., description="Job title")
    department: str = Field(..., description="Department")
    division: Optional[str] = Field(None, description="Division")
    area: Optional[str] = Field(None, description="Area")
    location: str = Field(..., description="Location")
    start_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    target_hire_date: Optional[str] = Field(None, description="Target hire date (YYYY-MM-DD)")
    days_open: int = Field(..., description="Number of days open")
    candidate_stage: Optional[str] = Field(None, description="Highest ranked candidate stage")

    class Config:
        json_schema_extra = {
            "example": {
                "requisition": "REQ-2025-001",
                "job_title": "Senior Software Engineer",
                "department": "Engineering",
                "location": "Scottsdale, AZ",
                "start_date": "2025-09-15",
                "target_hire_date": "2025-12-15",
                "days_open": 143
            }
        }


class AnalyticsResponse(BaseModel):
    """Complete analytics response with all dashboard data"""
    summary: RequisitionSummary
    aging: List[AgingBucket]
    departments: List[DepartmentStats]
    locations: List[LocationStats]
    reasons: List[RequisitionReason]
    trend: List[TrendDataPoint]
    critical: List[CriticalRequisition]

    class Config:
        json_schema_extra = {
            "example": {
                "summary": {
                    "total_open": 125,
                    "departments": 15,
                    "locations": 20,
                    "avg_days_open": 45.3,
                    "max_days_open": 180,
                    "total_positions": 125
                },
                "aging": [
                    {"category": "0-14 Days", "count": 30},
                    {"category": "15-30 Days", "count": 42}
                ],
                "departments": [
                    {"department": "Sales", "count": 25, "avg_days": 38.5}
                ],
                "locations": [
                    {"location": "Phoenix, AZ", "count": 18, "avg_days": 42.1}
                ],
                "reasons": [
                    {"reason": "New Position", "count": 45}
                ],
                "trend": [
                    {"month": "2026-01", "created": 35, "filled": 22, "still_open": 13}
                ],
                "critical": [
                    {
                        "requisition": "REQ-2025-001",
                        "job_title": "Senior Software Engineer",
                        "department": "Engineering",
                        "location": "Scottsdale, AZ",
                        "start_date": "2025-09-15",
                        "days_open": 143
                    }
                ]
            }
        }
