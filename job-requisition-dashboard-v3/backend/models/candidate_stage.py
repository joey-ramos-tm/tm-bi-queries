"""
Pydantic models for candidate stage data
"""
from pydantic import BaseModel, Field
from typing import Optional


class CandidateStageRecord(BaseModel):
    """Individual candidate stage record"""
    team_member_id: Optional[str] = Field(None, description="Team Member ID")
    job_title: Optional[str] = Field(None, description="Job Title")
    job_req: Optional[str] = Field(None, description="Full Job Requisition")
    job_req_number: Optional[str] = Field(None, description="Job Requisition Number")
    stage_date: Optional[str] = Field(None, description="Stage Date")
    candidate_name: Optional[str] = Field(None, description="Candidate Name")
    candidate_id: Optional[str] = Field(None, description="Candidate ID")
    job_profile_descriptor: Optional[str] = Field(None, description="Job Profile Descriptor")
    business_title: Optional[str] = Field(None, description="Business Title")
    current_stage_descriptor: Optional[str] = Field(None, description="Current Stage")
    division: Optional[str] = Field(None, description="Division")
    area: Optional[str] = Field(None, description="Area")
    job_family_group_name: Optional[str] = Field(None, description="Job Family Group Name")
    row_create_tms: Optional[str] = Field(None, description="Row Create Timestamp")

    class Config:
        json_schema_extra = {
            "example": {
                "team_member_id": "12345",
                "job_title": "Software Engineer",
                "job_req": "REQ-2025-001 Software Engineer",
                "job_req_number": "REQ-2025-001",
                "stage_date": "2025-12-01",
                "candidate_name": "John Doe",
                "candidate_id": "C12345",
                "job_profile_descriptor": "Engineer",
                "business_title": "Senior Engineer",
                "current_stage_descriptor": "Interview",
                "row_create_tms": "2025-12-01T10:00:00"
            }
        }


class CandidateStageSummary(BaseModel):
    """Summary statistics for candidate stages"""
    total_candidates: int = Field(..., description="Total number of candidates")
    total_requisitions: int = Field(..., description="Number of unique requisitions")
    stage_counts: dict = Field(..., description="Count of candidates by stage")

    class Config:
        json_schema_extra = {
            "example": {
                "total_candidates": 150,
                "total_requisitions": 45,
                "stage_counts": {
                    "Application Review": 30,
                    "Phone Screen": 25,
                    "Interview": 40,
                    "Offer": 20,
                    "Hired": 35
                }
            }
        }


class StageStats(BaseModel):
    """Statistics for a specific stage"""
    stage: str = Field(..., description="Stage name")
    count: int = Field(..., description="Number of candidates in this stage")
    percentage: float = Field(..., description="Percentage of total candidates")

    class Config:
        json_schema_extra = {
            "example": {
                "stage": "Interview",
                "count": 40,
                "percentage": 26.7
            }
        }


class RequisitionCandidateCount(BaseModel):
    """Candidate count by requisition"""
    job_req_number: str = Field(..., description="Job Requisition Number")
    job_title: Optional[str] = Field(None, description="Job Title")
    candidate_count: int = Field(..., description="Number of candidates")
    stages: dict = Field(..., description="Count by stage")

    class Config:
        json_schema_extra = {
            "example": {
                "job_req_number": "REQ-2025-001",
                "job_title": "Software Engineer",
                "candidate_count": 12,
                "stages": {
                    "Interview": 5,
                    "Offer": 3,
                    "Hired": 4
                }
            }
        }
