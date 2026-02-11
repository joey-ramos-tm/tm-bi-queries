"""
Candidate Stage Service
Business logic for candidate stage analytics
"""
from typing import List, Dict
from services.database import execute_query
from models.candidate_stage import (
    CandidateStageRecord,
    CandidateStageSummary,
    StageStats,
    RequisitionCandidateCount
)


class CandidateStageService:
    """Service for candidate stage data"""

    @staticmethod
    def get_summary(area: str = None, division: str = None, department: str = None, stage: str = None) -> CandidateStageSummary:
        """
        Get summary statistics for all candidates

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department filter
            stage: Optional stage filter

        Returns:
            CandidateStageSummary: Summary statistics
        """
        # Build where clause
        where_conditions = []
        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")
        if stage:
            where_conditions.append(f"CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor LIKE '%{stage}%'")

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        # Get total counts
        total_query = f"""
            SELECT
                COUNT(*) AS Total_Candidates,
                COUNT(DISTINCT JobReq_Number) AS Total_Requisitions
            FROM [Demo].[JobReqCandidateStage]
            WHERE {where_clause}
        """

        # Get stage counts
        stage_query = f"""
            SELECT
                COALESCE(CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor, 'Unknown') AS Stage,
                COUNT(*) AS Count
            FROM [Demo].[JobReqCandidateStage]
            WHERE {where_clause}
            GROUP BY CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor
            ORDER BY Count DESC
        """

        total_results = execute_query(total_query)
        stage_results = execute_query(stage_query)

        if not total_results:
            return CandidateStageSummary(
                total_candidates=0,
                total_requisitions=0,
                stage_counts={}
            )

        total_row = total_results[0]
        stage_counts = {row['Stage']: row['Count'] for row in stage_results}

        return CandidateStageSummary(
            total_candidates=total_row.get('Total_Candidates', 0) or 0,
            total_requisitions=total_row.get('Total_Requisitions', 0) or 0,
            stage_counts=stage_counts
        )

    @staticmethod
    def get_stage_breakdown(area: str = None, division: str = None, department: str = None, stage: str = None) -> List[StageStats]:
        """
        Get breakdown of candidates by stage with percentages

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department filter
            stage: Optional stage filter

        Returns:
            List[StageStats]: List of stage statistics
        """
        # Build where clause
        where_conditions = []
        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")
        if stage:
            where_conditions.append(f"CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor LIKE '%{stage}%'")

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        query = f"""
            WITH StageCounts AS (
                SELECT
                    COALESCE(CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor, 'Unknown') AS Stage,
                    COUNT(*) AS Count
                FROM [Demo].[JobReqCandidateStage]
                WHERE {where_clause}
                GROUP BY CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor
            ),
            TotalCount AS (
                SELECT COUNT(*) AS Total FROM [Demo].[JobReqCandidateStage] WHERE {where_clause}
            )
            SELECT
                sc.Stage,
                sc.Count,
                CAST(sc.Count * 100.0 / tc.Total AS DECIMAL(5,1)) AS Percentage
            FROM StageCounts sc
            CROSS JOIN TotalCount tc
            ORDER BY sc.Count DESC
        """

        results = execute_query(query)
        return [
            StageStats(
                stage=row.get('Stage', 'Unknown') or 'Unknown',
                count=row.get('Count', 0) or 0,
                percentage=float(row.get('Percentage', 0.0) or 0.0)
            )
            for row in results
        ]

    @staticmethod
    def get_requisition_candidates(area: str = None, division: str = None, department: str = None, stage: str = None) -> List[RequisitionCandidateCount]:
        """
        Get candidate counts by requisition

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department filter
            stage: Optional stage filter

        Returns:
            List[RequisitionCandidateCount]: List of requisition candidate counts
        """
        # Build where clause
        where_conditions = ["JobReq_Number IS NOT NULL"]
        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")
        if stage:
            where_conditions.append(f"CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor LIKE '%{stage}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 20
                JobReq_Number,
                MAX(JobTitle) AS Job_Title,
                COUNT(*) AS Candidate_Count
            FROM [Demo].[JobReqCandidateStage]
            WHERE {where_clause}
            GROUP BY JobReq_Number
            ORDER BY Candidate_Count DESC
        """

        results = execute_query(query)

        # For each requisition, get stage breakdown
        requisition_list = []
        for row in results:
            job_req = row.get('JobReq_Number')

            stage_query = f"""
                SELECT
                    COALESCE(CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor, 'Unknown') AS Stage,
                    COUNT(*) AS Count
                FROM [Demo].[JobReqCandidateStage]
                WHERE JobReq_Number = '{job_req}' AND {where_clause}
                GROUP BY CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor
            """

            stage_results = execute_query(stage_query)
            stages = {s['Stage']: s['Count'] for s in stage_results}

            requisition_list.append(
                RequisitionCandidateCount(
                    job_req_number=job_req or 'Unknown',
                    job_title=row.get('Job_Title'),
                    candidate_count=row.get('Candidate_Count', 0) or 0,
                    stages=stages
                )
            )

        return requisition_list

    @staticmethod
    def get_filter_options() -> Dict:
        """
        Get distinct values for filter dropdowns

        Returns:
            Dict with lists of areas, divisions, and departments
        """
        area_query = """
            SELECT DISTINCT Area
            FROM [Demo].[JobReqCandidateStage]
            WHERE Area IS NOT NULL
            ORDER BY Area
        """

        division_query = """
            SELECT DISTINCT Division
            FROM [Demo].[JobReqCandidateStage]
            WHERE Division IS NOT NULL
            ORDER BY Division
        """

        department_query = """
            SELECT DISTINCT Job_Family_Group_Name
            FROM [Demo].[JobReqCandidateStage]
            WHERE Job_Family_Group_Name IS NOT NULL
            ORDER BY Job_Family_Group_Name
        """

        stage_query = """
            SELECT DISTINCT CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor
            FROM [Demo].[JobReqCandidateStage]
            WHERE CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor IS NOT NULL
            ORDER BY CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor
        """

        areas = execute_query(area_query)
        divisions = execute_query(division_query)
        departments = execute_query(department_query)
        stages = execute_query(stage_query)

        return {
            'areas': [row['Area'] for row in areas],
            'divisions': [row['Division'] for row in divisions],
            'departments': [row['Job_Family_Group_Name'] for row in departments],
            'stages': [row['CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor'] for row in stages]
        }

    @staticmethod
    def get_all_candidates(
        page: int = 1,
        page_size: int = 50,
        stage: str = None,
        job_req: str = None,
        area: str = None,
        division: str = None,
        department: str = None
    ) -> Dict:
        """
        Get all candidates with pagination and filtering

        Args:
            page: Page number
            page_size: Number of records per page
            stage: Optional stage filter
            job_req: Optional job requisition filter
            area: Optional area filter
            division: Optional division filter
            department: Optional department filter

        Returns:
            Dict with candidates list and pagination info
        """
        where_conditions = []

        if stage:
            where_conditions.append(f"CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor LIKE '%{stage}%'")
        if job_req:
            where_conditions.append(f"JobReq_Number LIKE '%{job_req}%'")
        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        # Get total count
        count_query = f"""
            SELECT COUNT(*) AS Total
            FROM [Demo].[JobReqCandidateStage]
            WHERE {where_clause}
        """

        count_results = execute_query(count_query)
        total = count_results[0]['Total'] if count_results else 0

        # Get paginated data
        offset = (page - 1) * page_size
        data_query = f"""
            SELECT
                TeamMemberID,
                JobTitle,
                JobReq,
                JobReq_Number,
                CF_ESI_Latest_Candidate_Stage_group_StageDate AS Stage_Date,
                CandidateName,
                CandidateID,
                JobProfile_Descriptor,
                BusinessTitle,
                CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor AS Current_Stage,
                Division,
                Area,
                Job_Family_Group_Name,
                ROW_CREATE_TMS
            FROM [Demo].[JobReqCandidateStage]
            WHERE {where_clause}
            ORDER BY ROW_CREATE_TMS DESC
            OFFSET {offset} ROWS
            FETCH NEXT {page_size} ROWS ONLY
        """

        results = execute_query(data_query)

        candidates = [
            CandidateStageRecord(
                team_member_id=row.get('TeamMemberID'),
                job_title=row.get('JobTitle'),
                job_req=row.get('JobReq'),
                job_req_number=row.get('JobReq_Number'),
                stage_date=row.get('Stage_Date').strftime('%Y-%m-%d') if row.get('Stage_Date') else None,
                candidate_name=row.get('CandidateName'),
                candidate_id=row.get('CandidateID'),
                job_profile_descriptor=row.get('JobProfile_Descriptor'),
                business_title=row.get('BusinessTitle'),
                current_stage_descriptor=row.get('Current_Stage'),
                division=row.get('Division'),
                area=row.get('Area'),
                job_family_group_name=row.get('Job_Family_Group_Name'),
                row_create_tms=row.get('ROW_CREATE_TMS').strftime('%Y-%m-%d %H:%M:%S') if row.get('ROW_CREATE_TMS') else None
            )
            for row in results
        ]

        return {
            'candidates': candidates,
            'total': total,
            'page': page,
            'page_size': page_size,
            'total_pages': (total + page_size - 1) // page_size
        }
