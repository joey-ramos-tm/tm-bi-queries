"""
Candidate Stage API Routes
Endpoints for candidate stage analytics
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from models.candidate_stage import (
    CandidateStageSummary,
    StageStats,
    RequisitionCandidateCount,
    CandidateStageRecord
)
from services.candidate_stage_service import CandidateStageService

router = APIRouter()


@router.get("/summary", response_model=CandidateStageSummary)
async def get_summary(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department"),
    stage: Optional[str] = Query(None, description="Filter by stage")
):
    """
    Get summary statistics for candidate stages

    Returns:
        - Total candidates
        - Total unique requisitions
        - Count of candidates by stage
    """
    try:
        return CandidateStageService.get_summary(area, division, department, stage)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")


@router.get("/stage-breakdown", response_model=List[StageStats])
async def get_stage_breakdown(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department"),
    stage: Optional[str] = Query(None, description="Filter by stage")
):
    """
    Get breakdown of candidates by stage with percentages

    Returns list of stages with:
        - Stage name
        - Count of candidates
        - Percentage of total
    """
    try:
        return CandidateStageService.get_stage_breakdown(area, division, department, stage)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stage breakdown: {str(e)}")


@router.get("/requisition-candidates", response_model=List[RequisitionCandidateCount])
async def get_requisition_candidates(
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department"),
    stage: Optional[str] = Query(None, description="Filter by stage")
):
    """
    Get top 20 requisitions with candidate counts

    Returns:
        - Job requisition number
        - Job title
        - Total candidate count
        - Breakdown by stage
    """
    try:
        return CandidateStageService.get_requisition_candidates(area, division, department, stage)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching requisition candidates: {str(e)}")


@router.get("/filter-options")
async def get_filter_options():
    """
    Get distinct values for filter dropdowns

    Returns:
        - List of areas
        - List of divisions
        - List of departments
        - List of stages
    """
    try:
        return CandidateStageService.get_filter_options()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching filter options: {str(e)}")


@router.get("/candidates")
async def get_candidates(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Page size"),
    stage: Optional[str] = Query(None, description="Filter by stage"),
    job_req: Optional[str] = Query(None, description="Filter by job requisition"),
    area: Optional[str] = Query(None, description="Filter by area"),
    division: Optional[str] = Query(None, description="Filter by division"),
    department: Optional[str] = Query(None, description="Filter by department")
):
    """
    Get all candidates with pagination and filtering

    Query Parameters:
        - page: Page number (default: 1)
        - page_size: Records per page (default: 50, max: 100)
        - stage: Filter by candidate stage
        - job_req: Filter by job requisition number
        - area: Filter by area
        - division: Filter by division
        - department: Filter by department (Job Family Group Name)

    Returns:
        - List of candidates
        - Pagination information
    """
    try:
        return CandidateStageService.get_all_candidates(
            page, page_size, stage, job_req, area, division, department
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching candidates: {str(e)}")
