"""
Filled Analytics Service
Business logic for filled requisitions analytics
"""
from typing import List
from services.database import execute_query
from models.filled_analytics import (
    FilledSummary,
    MonthlyFilledCount,
    DivisionFilledStats,
    DepartmentFilledStats,
    AreaFilledStats,
    FilledRequisition
)


class FilledAnalyticsService:
    """Service for filled requisitions analytics"""

    @staticmethod
    def get_filled_summary(area: str = None, division: str = None, department: str = None) -> FilledSummary:
        """
        Get summary statistics for filled requisitions in the last 13 months

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            FilledSummary: Summary statistics
        """
        where_conditions = [
            "Status_ID LIKE '%Filled%'",
            "Target_Hire_Date >= DATEADD(MONTH, -13, GETDATE())",
            "Target_Hire_Date IS NOT NULL",
            "Recruiting_Start_Date IS NOT NULL"
        ]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                COUNT(*) AS Total_Filled,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date)) AS Avg_Time_To_Fill,
                MIN(DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date)) AS Fastest_Fill,
                MAX(DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date)) AS Slowest_Fill,
                COUNT(DISTINCT Division) AS Total_Divisions,
                COUNT(DISTINCT Job_Family_Group_Name) AS Total_Departments
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
        """

        # Get median using PERCENTILE_CONT
        median_query = f"""
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date))
                OVER () AS Median_Time_To_Fill
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
        """

        results = execute_query(query)
        median_results = execute_query(median_query)

        if not results or results[0].get('Total_Filled', 0) == 0:
            return FilledSummary(
                total_filled=0,
                avg_time_to_fill=0.0,
                median_time_to_fill=0.0,
                fastest_fill=0,
                slowest_fill=0,
                total_divisions=0,
                total_departments=0
            )

        row = results[0]
        median = median_results[0].get('Median_Time_To_Fill', 0.0) if median_results else 0.0

        return FilledSummary(
            total_filled=row.get('Total_Filled', 0) or 0,
            avg_time_to_fill=round(row.get('Avg_Time_To_Fill', 0.0) or 0.0, 1),
            median_time_to_fill=round(float(median) if median else 0.0, 1),
            fastest_fill=row.get('Fastest_Fill', 0) or 0,
            slowest_fill=row.get('Slowest_Fill', 0) or 0,
            total_divisions=row.get('Total_Divisions', 0) or 0,
            total_departments=row.get('Total_Departments', 0) or 0
        )

    @staticmethod
    def get_monthly_trend(area: str = None, division: str = None, department: str = None) -> List[MonthlyFilledCount]:
        """
        Get monthly filled requisitions trend for the last 13 months

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[MonthlyFilledCount]: List of monthly filled counts
        """
        where_conditions = [
            "Status_ID LIKE '%Filled%'",
            "Target_Hire_Date >= DATEADD(MONTH, -13, GETDATE())",
            "Target_Hire_Date IS NOT NULL",
            "Recruiting_Start_Date IS NOT NULL"
        ]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                FORMAT(Target_Hire_Date, 'yyyy-MM') AS Month,
                COUNT(*) AS Filled_Count,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date)) AS Avg_Time_To_Fill
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            GROUP BY FORMAT(Target_Hire_Date, 'yyyy-MM')
            ORDER BY Month
        """

        results = execute_query(query)
        return [
            MonthlyFilledCount(
                month=row.get('Month', ''),
                filled_count=row.get('Filled_Count', 0),
                avg_time_to_fill=round(row.get('Avg_Time_To_Fill', 0.0) or 0.0, 1)
            )
            for row in results
        ]

    @staticmethod
    def get_division_breakdown(area: str = None, division: str = None, department: str = None) -> List[DivisionFilledStats]:
        """
        Get top 10 divisions by filled requisitions count

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[DivisionFilledStats]: List of division statistics
        """
        where_conditions = [
            "Status_ID LIKE '%Filled%'",
            "Target_Hire_Date >= DATEADD(MONTH, -13, GETDATE())",
            "Target_Hire_Date IS NOT NULL",
            "Recruiting_Start_Date IS NOT NULL",
            "Division IS NOT NULL"
        ]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 10
                Division,
                COUNT(*) AS Filled_Count,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date)) AS Avg_Time_To_Fill,
                (
                    SELECT COUNT(*)
                    FROM [Demo].[JobRequisition] AS open_reqs
                    WHERE open_reqs.Division = filled_reqs.Division
                        AND open_reqs.Status_ID LIKE '%Open%'
                        {' AND open_reqs.Area LIKE ''%' + area + '%''' if area else ''}
                        {' AND open_reqs.Job_Family_Group_Name LIKE ''%' + department + '%''' if department else ''}
                ) AS Open_Count
            FROM [Demo].[JobRequisition] AS filled_reqs
            WHERE {where_clause}
            GROUP BY Division
            ORDER BY Filled_Count DESC
        """

        results = execute_query(query)
        return [
            DivisionFilledStats(
                division=row.get('Division', 'Unknown') or 'Unknown',
                filled_count=row.get('Filled_Count', 0),
                avg_time_to_fill=round(row.get('Avg_Time_To_Fill', 0.0) or 0.0, 1),
                open_count=row.get('Open_Count', 0) or 0
            )
            for row in results
        ]

    @staticmethod
    def get_department_breakdown(area: str = None, division: str = None, department: str = None) -> List[DepartmentFilledStats]:
        """
        Get top 10 departments by filled requisitions count

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[DepartmentFilledStats]: List of department statistics
        """
        where_conditions = [
            "Status_ID LIKE '%Filled%'",
            "Target_Hire_Date >= DATEADD(MONTH, -13, GETDATE())",
            "Target_Hire_Date IS NOT NULL",
            "Recruiting_Start_Date IS NOT NULL",
            "Job_Family_Group_Name IS NOT NULL"
        ]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 10
                Job_Family_Group_Name AS Department,
                COUNT(*) AS Filled_Count,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date)) AS Avg_Time_To_Fill,
                (
                    SELECT COUNT(*)
                    FROM [Demo].[JobRequisition] AS open_reqs
                    WHERE open_reqs.Job_Family_Group_Name = filled_reqs.Job_Family_Group_Name
                        AND open_reqs.Status_ID LIKE '%Open%'
                        {' AND open_reqs.Area LIKE ''%' + area + '%''' if area else ''}
                        {' AND open_reqs.Division LIKE ''%' + division + '%''' if division else ''}
                ) AS Open_Count
            FROM [Demo].[JobRequisition] AS filled_reqs
            WHERE {where_clause}
            GROUP BY Job_Family_Group_Name
            ORDER BY Filled_Count DESC
        """

        results = execute_query(query)
        return [
            DepartmentFilledStats(
                department=row.get('Department', 'Unknown') or 'Unknown',
                filled_count=row.get('Filled_Count', 0),
                avg_time_to_fill=round(row.get('Avg_Time_To_Fill', 0.0) or 0.0, 1),
                open_count=row.get('Open_Count', 0) or 0
            )
            for row in results
        ]

    @staticmethod
    def get_area_breakdown(area: str = None, division: str = None, department: str = None) -> List[AreaFilledStats]:
        """
        Get top 10 areas by filled requisitions count

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter

        Returns:
            List[AreaFilledStats]: List of area statistics
        """
        where_conditions = [
            "Status_ID LIKE '%Filled%'",
            "Target_Hire_Date >= DATEADD(MONTH, -13, GETDATE())",
            "Target_Hire_Date IS NOT NULL",
            "Recruiting_Start_Date IS NOT NULL",
            "Area IS NOT NULL"
        ]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 10
                Area,
                COUNT(*) AS Filled_Count,
                AVG(DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date)) AS Avg_Time_To_Fill
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            GROUP BY Area
            ORDER BY Filled_Count DESC
        """

        results = execute_query(query)
        return [
            AreaFilledStats(
                area=row.get('Area', 'Unknown') or 'Unknown',
                filled_count=row.get('Filled_Count', 0),
                avg_time_to_fill=round(row.get('Avg_Time_To_Fill', 0.0) or 0.0, 1)
            )
            for row in results
        ]

    @staticmethod
    def get_recent_filled(area: str = None, division: str = None, department: str = None, limit: int = 20) -> List[FilledRequisition]:
        """
        Get recently filled requisitions

        Args:
            area: Optional area filter
            division: Optional division filter
            department: Optional department (Job_Family_Group_Name) filter
            limit: Maximum number of results to return

        Returns:
            List[FilledRequisition]: List of recently filled requisitions
        """
        where_conditions = [
            "Status_ID LIKE '%Filled%'",
            "Target_Hire_Date >= DATEADD(MONTH, -13, GETDATE())",
            "Target_Hire_Date IS NOT NULL",
            "Recruiting_Start_Date IS NOT NULL"
        ]

        if area:
            where_conditions.append(f"Area LIKE '%{area}%'")
        if division:
            where_conditions.append(f"Division LIKE '%{division}%'")
        if department:
            where_conditions.append(f"Job_Family_Group_Name LIKE '%{department}%'")

        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP {limit}
                Requisition,
                Job_Profile_Name AS Job_Title,
                Job_Family_Group_Name AS Department,
                Division,
                Area,
                Location_ID AS Location,
                Recruiting_Start_Date,
                Target_Hire_Date,
                DATEDIFF(DAY, Recruiting_Start_Date, Target_Hire_Date) AS Days_To_Fill
            FROM [Demo].[JobRequisition]
            WHERE {where_clause}
            ORDER BY Target_Hire_Date DESC
        """

        results = execute_query(query)
        return [
            FilledRequisition(
                requisition=row.get('Requisition', ''),
                job_title=row.get('Job_Title', 'Unknown') or 'Unknown',
                department=row.get('Department', 'Unknown') or 'Unknown',
                division=row.get('Division') or None,
                area=row.get('Area') or None,
                location=row.get('Location', 'Unknown') or 'Unknown',
                recruiting_start_date=row.get('Recruiting_Start_Date').strftime('%Y-%m-%d') if row.get('Recruiting_Start_Date') else 'N/A',
                filled_date=row.get('Target_Hire_Date').strftime('%Y-%m-%d') if row.get('Target_Hire_Date') else 'N/A',
                days_to_fill=row.get('Days_To_Fill', 0) or 0
            )
            for row in results
        ]
