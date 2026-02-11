"""
Pydantic models for filled requisitions analytics
"""
from pydantic import BaseModel, Field
from typing import List, Optional


class FilledSummary(BaseModel):
    """Summary statistics for filled requisitions"""
    total_filled: int = Field(..., description="Total number of requisitions filled in last 12 months")
    avg_time_to_fill: float = Field(..., description="Average days to fill a requisition")
    median_time_to_fill: float = Field(..., description="Median days to fill a requisition")
    fastest_fill: int = Field(..., description="Fastest time to fill (days)")
    slowest_fill: int = Field(..., description="Slowest time to fill (days)")
    total_divisions: int = Field(..., description="Number of divisions with filled requisitions")
    total_departments: int = Field(..., description="Number of departments with filled requisitions")

    class Config:
        json_schema_extra = {
            "example": {
                "total_filled": 245,
                "avg_time_to_fill": 42.5,
                "median_time_to_fill": 38.0,
                "fastest_fill": 7,
                "slowest_fill": 180,
                "total_divisions": 12,
                "total_departments": 18
            }
        }


class MonthlyFilledCount(BaseModel):
    """Monthly filled requisitions count"""
    month: str = Field(..., description="Month in YYYY-MM format")
    filled_count: int = Field(..., description="Number of requisitions filled in this month")
    avg_time_to_fill: float = Field(..., description="Average time to fill for this month")

    class Config:
        json_schema_extra = {
            "example": {
                "month": "2026-01",
                "filled_count": 22,
                "avg_time_to_fill": 45.3
            }
        }


class DivisionFilledStats(BaseModel):
    """Division filled statistics"""
    division: str = Field(..., description="Division name")
    filled_count: int = Field(..., description="Number of requisitions filled")
    avg_time_to_fill: float = Field(..., description="Average days to fill")
    open_count: int = Field(..., description="Current open requisitions")

    class Config:
        json_schema_extra = {
            "example": {
                "division": "Sales",
                "filled_count": 35,
                "avg_time_to_fill": 38.5,
                "open_count": 12
            }
        }


class DepartmentFilledStats(BaseModel):
    """Department filled statistics"""
    department: str = Field(..., description="Department name (Job Family Group)")
    filled_count: int = Field(..., description="Number of requisitions filled")
    avg_time_to_fill: float = Field(..., description="Average days to fill")
    open_count: int = Field(..., description="Current open requisitions")

    class Config:
        json_schema_extra = {
            "example": {
                "department": "Engineering",
                "filled_count": 28,
                "avg_time_to_fill": 52.1,
                "open_count": 8
            }
        }


class AreaFilledStats(BaseModel):
    """Area filled statistics"""
    area: str = Field(..., description="Area name")
    filled_count: int = Field(..., description="Number of requisitions filled")
    avg_time_to_fill: float = Field(..., description="Average days to fill")

    class Config:
        json_schema_extra = {
            "example": {
                "area": "Area President 1",
                "filled_count": 65,
                "avg_time_to_fill": 41.2
            }
        }


class FilledRequisition(BaseModel):
    """Individual filled requisition details"""
    requisition: str = Field(..., description="Requisition ID")
    job_title: str = Field(..., description="Job title")
    department: str = Field(..., description="Department")
    division: Optional[str] = Field(None, description="Division")
    area: Optional[str] = Field(None, description="Area")
    location: str = Field(..., description="Location")
    recruiting_start_date: str = Field(..., description="Recruiting start date (YYYY-MM-DD)")
    filled_date: str = Field(..., description="Filled date (YYYY-MM-DD)")
    days_to_fill: int = Field(..., description="Number of days to fill")

    class Config:
        json_schema_extra = {
            "example": {
                "requisition": "REQ-2025-001",
                "job_title": "Senior Software Engineer",
                "department": "Engineering",
                "division": "Technology",
                "area": "Area President 1",
                "location": "Scottsdale, AZ",
                "recruiting_start_date": "2025-09-15",
                "filled_date": "2025-11-30",
                "days_to_fill": 76
            }
        }


class FilledAnalyticsResponse(BaseModel):
    """Complete filled requisitions analytics response"""
    summary: FilledSummary
    monthly_trend: List[MonthlyFilledCount]
    by_division: List[DivisionFilledStats]
    by_department: List[DepartmentFilledStats]
    by_area: List[AreaFilledStats]
    recent_filled: List[FilledRequisition]

    class Config:
        json_schema_extra = {
            "example": {
                "summary": {
                    "total_filled": 245,
                    "avg_time_to_fill": 42.5,
                    "median_time_to_fill": 38.0,
                    "fastest_fill": 7,
                    "slowest_fill": 180,
                    "total_divisions": 12,
                    "total_departments": 18
                },
                "monthly_trend": [
                    {"month": "2026-01", "filled_count": 22, "avg_time_to_fill": 45.3}
                ],
                "by_division": [
                    {"division": "Sales", "filled_count": 35, "avg_time_to_fill": 38.5, "open_count": 12}
                ],
                "by_department": [
                    {"department": "Engineering", "filled_count": 28, "avg_time_to_fill": 52.1, "open_count": 8}
                ],
                "by_area": [
                    {"area": "Area President 1", "filled_count": 65, "avg_time_to_fill": 41.2}
                ],
                "recent_filled": [
                    {
                        "requisition": "REQ-2025-001",
                        "job_title": "Senior Software Engineer",
                        "department": "Engineering",
                        "division": "Technology",
                        "area": "Area President 1",
                        "location": "Scottsdale, AZ",
                        "recruiting_start_date": "2025-09-15",
                        "filled_date": "2025-11-30",
                        "days_to_fill": 76
                    }
                ]
            }
        }
