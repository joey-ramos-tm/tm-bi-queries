"""
Requisition Service
Business logic for requisition data retrieval and management
"""
from typing import List, Optional
import math
from services.database import execute_query
from models.requisition import (
    RequisitionListItem,
    RequisitionDetail,
    PaginatedResponse,
    RequisitionFilters
)


class RequisitionService:
    """Service for requisition data operations"""

    # Stage ranking lookup based on Excel file
    STAGE_RANKINGS = {
        'Ready for Hire': 8,
        'Pre-Employment Verification': 7,
        'Declined by Candidate': 6,
        'Offer': 5,
        'Interview': 4,
        'Manager Review Complete': 3,
        'Manager Review': 2,
        'New': 1
    }

    @staticmethod
    def get_max_ranked_stage_for_requisitions(requisitions: List[str]) -> dict:
        """
        Get the highest-ranked candidate stage for a list of requisitions

        Args:
            requisitions: List of requisition IDs

        Returns:
            dict: Map of requisition ID to highest-ranked stage
        """
        if not requisitions:
            return {}

        # Build query to get all candidates for these requisitions
        requisition_list = "','".join(requisitions)
        query = f"""
            SELECT
                JobReq_Number,
                CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor AS Current_Stage
            FROM [Demo].[JobReqCandidateStage]
            WHERE JobReq_Number IN ('{requisition_list}')
                AND CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor IS NOT NULL
        """

        results = execute_query(query)

        # Group by requisition and find max-ranked stage
        requisition_stages = {}
        for row in results:
            req_num = row.get('JobReq_Number')
            stage = row.get('Current_Stage')

            if not req_num or not stage:
                continue

            # Get ranking for this stage (default to 0 if not in our lookup)
            stage_rank = RequisitionService.STAGE_RANKINGS.get(stage, 0)

            # If we haven't seen this requisition yet, or this stage has a higher rank, update it
            if req_num not in requisition_stages:
                requisition_stages[req_num] = {'stage': stage, 'rank': stage_rank}
            elif stage_rank > requisition_stages[req_num]['rank']:
                requisition_stages[req_num] = {'stage': stage, 'rank': stage_rank}

        # Return just the stage names
        return {req: data['stage'] for req, data in requisition_stages.items()}

    @staticmethod
    def get_requisitions(filters: RequisitionFilters) -> PaginatedResponse[RequisitionListItem]:
        """
        Get paginated list of requisitions with optional filters

        Args:
            filters: Filter and pagination parameters

        Returns:
            PaginatedResponse[RequisitionListItem]: Paginated list of requisitions
        """
        # Build WHERE clause based on filters
        where_conditions = [
            "Status_ID LIKE '%Open%'"
        ]

        if filters.status:
            where_conditions.append(
                f"Status_ID LIKE '%{filters.status}%'"
            )

        if filters.department:
            where_conditions.append(
                f"Job_Family_Group_Name LIKE '%{filters.department}%'"
            )

        if filters.location:
            where_conditions.append(
                f"Location_ID LIKE '%{filters.location}%'"
            )

        if filters.min_days_open is not None:
            where_conditions.append(
                f"DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) >= {filters.min_days_open}"
            )

        if filters.max_days_open is not None:
            where_conditions.append(
                f"DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) <= {filters.max_days_open}"
            )

        where_clause = " AND ".join(where_conditions)

        # Build ORDER BY clause
        sort_field_map = {
            'requisition': 'Requisition',
            'job_title': 'Job_Title',
            'department': 'Department',
            'location': 'Location',
            'days_open': 'Days_Open',
            'start_date': 'Start_Date'
        }

        sort_field = sort_field_map.get(filters.sort_by, 'Days_Open')
        sort_order = 'ASC' if filters.sort_order == 'asc' else 'DESC'

        # Get total count
        count_query = f"""
            SELECT COUNT(*) AS Total
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
        """

        count_result = execute_query(count_query)
        total = count_result[0].get('Total', 0) if count_result else 0

        # Calculate pagination
        total_pages = math.ceil(total / filters.page_size) if total > 0 else 1
        offset = (filters.page - 1) * filters.page_size

        # Get paginated data
        data_query = f"""
            SELECT
                Requisition,
                Job_Profile_Name AS Job_Title,
                Job_Family_Group_Name AS Department,
                Location_ID AS Location,
                Status_ID AS Status,
                DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) AS Days_Open,
                Recruiting_Start_Date AS Start_Date,
                Target_Hire_Date
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            ORDER BY {sort_field} {sort_order}
            OFFSET {offset} ROWS
            FETCH NEXT {filters.page_size} ROWS ONLY
        """

        results = execute_query(data_query)

        # Get candidate stages for all requisitions in this page
        requisition_ids = [row.get('Requisition', '') for row in results]
        stage_map = RequisitionService.get_max_ranked_stage_for_requisitions(requisition_ids)

        items = [
            RequisitionListItem(
                requisition=row.get('Requisition', ''),
                job_title=row.get('Job_Title', 'Unknown') or 'Unknown',
                department=row.get('Department', 'Unknown') or 'Unknown',
                location=row.get('Location', 'Unknown') or 'Unknown',
                status=row.get('Status', 'Unknown') or 'Unknown',
                days_open=row.get('Days_Open', 0) or 0,
                start_date=row.get('Start_Date').strftime('%Y-%m-%d') if row.get('Start_Date') else None,
                target_hire_date=row.get('Target_Hire_Date').strftime('%Y-%m-%d') if row.get('Target_Hire_Date') else None,
                candidate_stage=stage_map.get(row.get('Requisition', ''))
            )
            for row in results
        ]

        return PaginatedResponse(
            data=items,
            total=total,
            page=filters.page,
            page_size=filters.page_size,
            total_pages=total_pages
        )

    @staticmethod
    def get_requisition_by_id(requisition_id: str) -> Optional[RequisitionDetail]:
        """
        Get detailed information for a specific requisition

        Args:
            requisition_id: Requisition ID

        Returns:
            Optional[RequisitionDetail]: Requisition details or None if not found
        """
        query = """
            SELECT
                Requisition,
                Job_Profile_Name AS Job_Title,
                Job_Family_Group_Name AS Department,
                Location_ID AS Location,
                Status_ID AS Status,
                Recruiting_Start_Date,
                DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) AS Days_Open,
                'Not specified' AS Reason,
                Job_Profile_ID AS Job_Profile,
                Department_ID AS Supervisory_Org
            FROM [Demo].[JobRequisition]
            WHERE Requisition = ?
        """

        results = execute_query(query, (requisition_id,))

        if not results:
            return None

        row = results[0]
        return RequisitionDetail(
            requisition=row.get('Requisition', ''),
            job_title=row.get('Job_Title', 'Unknown') or 'Unknown',
            department=row.get('Department', 'Unknown') or 'Unknown',
            location=row.get('Location', 'Unknown') or 'Unknown',
            status=row.get('Status', 'Unknown') or 'Unknown',
            recruiting_start_date=row.get('Recruiting_Start_Date').strftime('%Y-%m-%d') if row.get('Recruiting_Start_Date') else None,
            days_open=row.get('Days_Open', 0) or 0,
            reason=row.get('Reason') or 'Not specified',
            hiring_manager='Not available',  # Column not in database
            job_profile=row.get('Job_Profile') or 'Not specified',
            supervisory_org=row.get('Supervisory_Org') or 'Not specified'
        )

    @staticmethod
    def get_all_requisitions_for_export(filters: Optional[RequisitionFilters] = None) -> List[RequisitionListItem]:
        """
        Get all requisitions for export (no pagination)

        Args:
            filters: Optional filters to apply

        Returns:
            List[RequisitionListItem]: List of all requisitions matching filters
        """
        if filters is None:
            filters = RequisitionFilters()

        # Build WHERE clause (same as get_requisitions)
        where_conditions = [
            "Status_ID LIKE '%Open%'"
        ]

        if filters.status:
            where_conditions.append(
                f"Status_ID LIKE '%{filters.status}%'"
            )

        if filters.department:
            where_conditions.append(
                f"Job_Family_Group_Name LIKE '%{filters.department}%'"
            )

        if filters.location:
            where_conditions.append(
                f"Location_ID LIKE '%{filters.location}%'"
            )

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                Requisition,
                Job_Profile_Name AS Job_Title,
                Job_Family_Group_Name AS Department,
                Location_ID AS Location,
                Status_ID AS Status,
                DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) AS Days_Open,
                Recruiting_Start_Date AS Start_Date,
                Target_Hire_Date
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            ORDER BY Days_Open DESC
        """

        results = execute_query(query)

        # Get candidate stages for all requisitions
        requisition_ids = [row.get('Requisition', '') for row in results]
        stage_map = RequisitionService.get_max_ranked_stage_for_requisitions(requisition_ids)

        return [
            RequisitionListItem(
                requisition=row.get('Requisition', ''),
                job_title=row.get('Job_Title', 'Unknown') or 'Unknown',
                department=row.get('Department', 'Unknown') or 'Unknown',
                location=row.get('Location', 'Unknown') or 'Unknown',
                status=row.get('Status', 'Unknown') or 'Unknown',
                days_open=row.get('Days_Open', 0) or 0,
                start_date=row.get('Start_Date').strftime('%Y-%m-%d') if row.get('Start_Date') else None,
                target_hire_date=row.get('Target_Hire_Date').strftime('%Y-%m-%d') if row.get('Target_Hire_Date') else None,
                candidate_stage=stage_map.get(row.get('Requisition', ''))
            )
            for row in results
        ]
