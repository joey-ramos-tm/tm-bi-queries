"""
Analytics Service
Business logic for dashboard analytics and statistics
"""
from typing import List, Dict
from services.database import execute_query
from models.analytics import (
    RequisitionSummary,
    AgingBucket,
    DepartmentStats,
    LocationStats,
    RequisitionReason,
    TrendDataPoint,
    CriticalRequisition
)
from services.requisition_service import RequisitionService


class AnalyticsService:
    """Service for analytics and dashboard data"""

    @staticmethod
    def get_summary(area: str = None, division: str = None, department: str = None) -> RequisitionSummary:
        """
        Get summary statistics for open job requisitions

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            RequisitionSummary: Summary statistics
        """
        where_conditions = ["Status_ID LIKE '%Open%'"]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                COUNT(*) AS Total_Open,
                COUNT(DISTINCT Department_ID) AS Departments,
                COUNT(DISTINCT Location_ID) AS Locations,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, GETDATE())) AS Avg_Days_Open,
                MAX(DATEDIFF(DAY, Recruiting_Start_Date, GETDATE())) AS Max_Days_Open,
                COUNT(*) AS Total_Positions
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
        """

        # Get filled count for last 90 days
        filled_where_conditions = [
            "Status_ID LIKE '%Filled%'",
            "Recruiting_Start_Date >= DATEADD(DAY, -90, GETDATE())"
        ]
        if area:
            filled_where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            filled_where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            filled_where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        filled_where_clause = " AND ".join(filled_where_conditions)

        filled_query = f"""
            SELECT COUNT(*) AS Filled_Count
            FROM [Demo].[JobRequisition]
            WHERE {filled_where_clause}
        """

        results = execute_query(query)
        filled_results = execute_query(filled_query)

        if not results:
            return RequisitionSummary(
                total_open=0,
                departments=0,
                locations=0,
                avg_days_open=0.0,
                max_days_open=0,
                total_positions=0,
                filled_last_90_days=0
            )

        row = results[0]
        filled_row = filled_results[0] if filled_results else {}

        return RequisitionSummary(
            total_open=row.get('Total_Open', 0) or 0,
            departments=row.get('Departments', 0) or 0,
            locations=row.get('Locations', 0) or 0,
            avg_days_open=round(row.get('Avg_Days_Open', 0.0) or 0.0, 1),
            max_days_open=row.get('Max_Days_Open', 0) or 0,
            total_positions=row.get('Total_Positions', 0) or 0,
            filled_last_90_days=filled_row.get('Filled_Count', 0) or 0
        )

    @staticmethod
    def get_aging_analysis(area: str = None, division: str = None, department: str = None) -> List[AgingBucket]:
        """
        Get aging analysis (requisitions grouped by age buckets)

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[AgingBucket]: List of aging buckets
        """
        where_conditions = ["Status_ID LIKE '%Open%'"]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                Age_Category,
                COUNT(*) AS Count
            FROM (
                SELECT
                    CASE
                        WHEN DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) <= 14
                        THEN '0-14 Days'
                        WHEN DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) <= 30
                        THEN '15-30 Days'
                        WHEN DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) <= 60
                        THEN '31-60 Days'
                        WHEN DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) <= 90
                        THEN '61-90 Days'
                        ELSE '90+ Days'
                    END AS Age_Category
                FROM [Demo].[JobRequisition]
                WHERE {where_clause}
            ) AS AgeData
            GROUP BY Age_Category
            ORDER BY
                CASE Age_Category
                    WHEN '0-14 Days' THEN 1
                    WHEN '15-30 Days' THEN 2
                    WHEN '31-60 Days' THEN 3
                    WHEN '61-90 Days' THEN 4
                    WHEN '90+ Days' THEN 5
                END
        """

        results = execute_query(query)
        return [
            AgingBucket(
                category=row.get('Age_Category', 'Unknown'),
                count=row.get('Count', 0)
            )
            for row in results
        ]

    @staticmethod
    def get_department_breakdown(area: str = None, division: str = None, department: str = None) -> List[DepartmentStats]:
        """
        Get top 10 divisions with open requisitions

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[DepartmentStats]: List of division statistics
        """
        where_conditions = ["Status_ID LIKE '%Open%'", "Division IS NOT NULL"]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 10
                Division AS Department,
                COUNT(*) AS Open_Count,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, GETDATE())) AS Avg_Days_Open,
                (
                    SELECT COUNT(*)
                    FROM [Demo].[JobRequisition] AS filled
                    WHERE filled.Division = open_reqs.Division
                        AND filled.Status_ID LIKE '%Filled%'
                        AND filled.Recruiting_Start_Date >= DATEADD(DAY, -90, GETDATE())
                        {' AND filled.Area LIKE ''%' + area + '%''' if area else ''}
                        {' AND filled.Job_Family_Group_Name LIKE ''%' + department + '%''' if department else ''}
                ) AS Filled_Last_30_Days
            FROM [Demo].[JobRequisition] AS open_reqs
            WHERE {where_clause}
            GROUP BY Division
            ORDER BY Open_Count DESC
        """

        results = execute_query(query)
        return [
            DepartmentStats(
                department=row.get('Department', 'Unknown') or 'Unknown',
                count=row.get('Open_Count', 0),
                avg_days=round(row.get('Avg_Days_Open', 0.0) or 0.0, 1),
                filled_last_90_days=row.get('Filled_Last_30_Days', 0) or 0
            )
            for row in results
        ]

    @staticmethod
    def get_location_breakdown(area: str = None, division: str = None, department: str = None) -> List[LocationStats]:
        """
        Get top 10 departments (Job Family Groups) with open requisitions

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[LocationStats]: List of department statistics
        """
        where_conditions = ["Status_ID LIKE '%Open%'", "Job_Family_Group_Name IS NOT NULL"]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 10
                Job_Family_Group_Name AS Location,
                COUNT(*) AS Open_Count,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, GETDATE())) AS Avg_Days_Open,
                (
                    SELECT COUNT(*)
                    FROM [Demo].[JobRequisition] AS filled
                    WHERE filled.Job_Family_Group_Name = open_reqs.Job_Family_Group_Name
                        AND filled.Status_ID LIKE '%Filled%'
                        AND filled.Recruiting_Start_Date >= DATEADD(DAY, -90, GETDATE())
                        {' AND filled.Area LIKE ''%' + area + '%''' if area else ''}
                        {' AND filled.Division LIKE ''%' + division + '%''' if division else ''}
                ) AS Filled_Last_30_Days
            FROM [Demo].[JobRequisition] AS open_reqs
            WHERE {where_clause}
            GROUP BY Job_Family_Group_Name
            ORDER BY Open_Count DESC
        """

        results = execute_query(query)
        return [
            LocationStats(
                location=row.get('Location', 'Unknown') or 'Unknown',
                count=row.get('Open_Count', 0),
                avg_days=round(row.get('Avg_Days_Open', 0.0) or 0.0, 1),
                filled_last_90_days=row.get('Filled_Last_30_Days', 0) or 0
            )
            for row in results
        ]

    @staticmethod
    def get_requisition_reasons(area: str = None, division: str = None, department: str = None) -> List[RequisitionReason]:
        """
        Get top areas with open requisitions

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[RequisitionReason]: List of area statistics
        """
        where_conditions = ["Status_ID LIKE '%Open%'", "Area IS NOT NULL"]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 10
                Area AS Reason,
                COUNT(*) AS Count
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            GROUP BY Area
            ORDER BY Count DESC
        """

        results = execute_query(query)
        return [
            RequisitionReason(
                reason=row.get('Reason', 'Unknown') or 'Unknown',
                count=row.get('Count', 0)
            )
            for row in results
        ]

    @staticmethod
    def get_monthly_trend(area: str = None, division: str = None, department: str = None) -> List[TrendDataPoint]:
        """
        Get monthly trend data (last 12 months)

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[TrendDataPoint]: List of monthly trend data points
        """
        where_conditions = ["Recruiting_Start_Date >= DATEADD(MONTH, -12, GETDATE())"]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                FORMAT(Recruiting_Start_Date, 'yyyy-MM') AS Month,
                COUNT(*) AS Created,
                SUM(CASE WHEN Status_ID LIKE '%Filled%'
                         THEN 1 ELSE 0 END) AS Filled,
                SUM(CASE WHEN Status_ID LIKE '%Open%'
                         THEN 1 ELSE 0 END) AS Still_Open
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            GROUP BY FORMAT(Recruiting_Start_Date, 'yyyy-MM')
            ORDER BY Month
        """

        results = execute_query(query)
        return [
            TrendDataPoint(
                month=row.get('Month', ''),
                created=row.get('Created', 0),
                filled=row.get('Filled', 0),
                still_open=row.get('Still_Open', 0)
            )
            for row in results
        ]

    @staticmethod
    def get_critical_requisitions(area: str = None, division: str = None, department: str = None) -> List[CriticalRequisition]:
        """
        Get critical requisitions (90+ days open)

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[CriticalRequisition]: List of critical requisitions
        """
        where_conditions = [
            "Status_ID LIKE '%Open%'",
            "DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) > 90"
        ]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 20
                Requisition,
                Job_Profile_Name AS Job_Title,
                Job_Family_Group_Name AS Department,
                Division,
                Area,
                Location_ID AS Location,
                Recruiting_Start_Date AS Start_Date,
                Target_Hire_Date,
                DATEDIFF(DAY, Recruiting_Start_Date, GETDATE()) AS Days_Open
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            ORDER BY Days_Open DESC
        """

        results = execute_query(query)

        # Get candidate stages for all critical requisitions
        requisition_ids = [row.get('Requisition', '') for row in results]
        stage_map = RequisitionService.get_max_ranked_stage_for_requisitions(requisition_ids)

        return [
            CriticalRequisition(
                requisition=row.get('Requisition', ''),
                job_title=row.get('Job_Title', 'Unknown') or 'Unknown',
                department=row.get('Department', 'Unknown') or 'Unknown',
                division=row.get('Division') or None,
                area=row.get('Area') or None,
                location=row.get('Location', 'Unknown') or 'Unknown',
                start_date=row.get('Start_Date').strftime('%Y-%m-%d') if row.get('Start_Date') else 'N/A',
                target_hire_date=row.get('Target_Hire_Date').strftime('%Y-%m-%d') if row.get('Target_Hire_Date') else None,
                days_open=row.get('Days_Open', 0) or 0,
                candidate_stage=stage_map.get(row.get('Requisition', ''))
            )
            for row in results
        ]
