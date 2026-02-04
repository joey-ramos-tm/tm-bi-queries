"""
Job Requisition Dashboard API
Flask backend to serve Job Requisition data for HTML dashboard
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from sql_connection import connect_to_datalake
import traceback
from datetime import datetime, timedelta
from functools import lru_cache
import time

app = Flask(__name__)
CORS(app)

# Cache configuration - refresh every 5 minutes
CACHE_DURATION = 300  # 5 minutes
cache_timestamp = None
cached_data = {}


def get_cached_data():
    """Get cached data or refresh if expired"""
    global cache_timestamp, cached_data

    current_time = time.time()

    if cache_timestamp is None or (current_time - cache_timestamp) > CACHE_DURATION:
        print(f"Cache expired or empty. Refreshing data... (Last refresh: {cache_timestamp})")
        cached_data = fetch_all_data()
        cache_timestamp = current_time
        print(f"Cache refreshed at {datetime.now()}")
    else:
        time_remaining = CACHE_DURATION - (current_time - cache_timestamp)
        print(f"Using cached data. Refresh in {time_remaining:.0f} seconds")

    return cached_data


def fetch_all_data():
    """Fetch all dashboard data from database"""
    data = {}

    try:
        conn = connect_to_datalake()
        cursor = conn.cursor()

        # Query 1: Summary Statistics
        print("Fetching summary statistics...")
        cursor.execute("""
            SELECT
                COUNT(*) AS Total_Open,
                COUNT(DISTINCT JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"')) AS Departments,
                COUNT(DISTINCT JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) AS Locations,
                AVG(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open,
                MAX(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Max_Days_Open,
                COUNT(*) AS Total_Positions
            FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
            WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
        """)

        row = cursor.fetchone()
        data['summary'] = {
            'total_open': row[0] or 0,
            'departments': row[1] or 0,
            'locations': row[2] or 0,
            'avg_days_open': round(row[3] or 0, 1),
            'max_days_open': row[4] or 0,
            'total_positions': row[5] or 0
        }

        # Query 2: Aging Analysis
        print("Fetching aging analysis...")
        cursor.execute("""
            SELECT
                Age_Category,
                COUNT(*) AS Count
            FROM (
                SELECT
                    CASE
                        WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 14
                        THEN '0-14 Days'
                        WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 30
                        THEN '15-30 Days'
                        WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 60
                        THEN '31-60 Days'
                        WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 90
                        THEN '61-90 Days'
                        ELSE '90+ Days'
                    END AS Age_Category
                FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
                WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
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
        """)

        data['aging'] = [{'category': row[0], 'count': row[1]} for row in cursor.fetchall()]

        # Query 3: Department Breakdown (Top 10)
        print("Fetching department breakdown...")
        cursor.execute("""
            SELECT TOP 10
                JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
                COUNT(*) AS Open_Count,
                AVG(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open
            FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
            WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                AND JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') IS NOT NULL
            GROUP BY JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"')
            ORDER BY Open_Count DESC
        """)

        data['departments'] = [
            {
                'department': row[0] or 'Unknown',
                'count': row[1],
                'avg_days': round(row[2] or 0, 1)
            }
            for row in cursor.fetchall()
        ]

        # Query 4: Location Breakdown (Top 10)
        print("Fetching location breakdown...")
        cursor.execute("""
            SELECT TOP 10
                JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
                COUNT(*) AS Open_Count,
                AVG(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open
            FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
            WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                AND JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') IS NOT NULL
            GROUP BY JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')
            ORDER BY Open_Count DESC
        """)

        data['locations'] = [
            {
                'location': row[0] or 'Unknown',
                'count': row[1],
                'avg_days': round(row[2] or 0, 1)
            }
            for row in cursor.fetchall()
        ]

        # Query 5: Requisition Reasons
        print("Fetching requisition reasons...")
        cursor.execute("""
            SELECT
                JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Reason,
                COUNT(*) AS Count
            FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
            WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                AND JSON_VALUE(RequisitionReason, '$[1]."#text"') IS NOT NULL
            GROUP BY JSON_VALUE(RequisitionReason, '$[1]."#text"')
            ORDER BY Count DESC
        """)

        data['reasons'] = [
            {'reason': row[0] or 'Unknown', 'count': row[1]}
            for row in cursor.fetchall()
        ]

        # Query 6: Hiring Manager Workload - SKIPPED (column not available)
        print("Skipping hiring manager workload (column not available)...")
        data['managers'] = []

        # Query 7: Monthly Trend (Last 12 months)
        print("Fetching monthly trend...")
        cursor.execute("""
            SELECT
                FORMAT(TRY_CAST(Recruiting_Start_Date AS DATE), 'yyyy-MM') AS Month,
                COUNT(*) AS Created,
                SUM(CASE WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Filled%'
                         THEN 1 ELSE 0 END) AS Filled,
                SUM(CASE WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                         THEN 1 ELSE 0 END) AS Still_Open
            FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
            WHERE TRY_CAST(Recruiting_Start_Date AS DATE) >= DATEADD(MONTH, -12, GETDATE())
            GROUP BY FORMAT(TRY_CAST(Recruiting_Start_Date AS DATE), 'yyyy-MM')
            ORDER BY Month
        """)

        data['trend'] = [
            {
                'month': row[0],
                'created': row[1],
                'filled': row[2],
                'still_open': row[3]
            }
            for row in cursor.fetchall()
        ]

        # Query 8: Critical Requisitions (90+ days)
        print("Fetching critical requisitions...")
        cursor.execute("""
            SELECT TOP 20
                Requisition,
                JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
                JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
                JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
                TRY_CAST(Recruiting_Start_Date AS DATE) AS Start_Date,
                DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) AS Days_Open
            FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
            WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                AND DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) > 90
            ORDER BY Days_Open DESC
        """)

        data['critical'] = [
            {
                'requisition': row[0],
                'job_title': row[1] or 'Unknown',
                'department': row[2] or 'Unknown',
                'location': row[3] or 'Unknown',
                'start_date': row[4].strftime('%Y-%m-%d') if row[4] else 'N/A',
                'days_open': row[5] or 0
            }
            for row in cursor.fetchall()
        ]

        cursor.close()
        conn.close()

        print("All data fetched successfully!")
        return data

    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        traceback.print_exc()
        return {
            'error': str(e),
            'summary': {'total_open': 0, 'departments': 0, 'locations': 0, 'avg_days_open': 0, 'max_days_open': 0, 'total_positions': 0},
            'aging': [],
            'departments': [],
            'locations': [],
            'reasons': [],
            'managers': [],
            'trend': [],
            'critical': []
        }


@app.route('/')
def index():
    """Serve the dashboard HTML"""
    return send_from_directory('.', 'job_requisition_dashboard.html')


@app.route('/api/summary')
def get_summary():
    """Get summary statistics"""
    data = get_cached_data()
    return jsonify(data.get('summary', {}))


@app.route('/api/aging')
def get_aging():
    """Get aging analysis"""
    data = get_cached_data()
    return jsonify(data.get('aging', []))


@app.route('/api/departments')
def get_departments():
    """Get department breakdown"""
    data = get_cached_data()
    return jsonify(data.get('departments', []))


@app.route('/api/locations')
def get_locations():
    """Get location breakdown"""
    data = get_cached_data()
    return jsonify(data.get('locations', []))


@app.route('/api/reasons')
def get_reasons():
    """Get requisition reasons"""
    data = get_cached_data()
    return jsonify(data.get('reasons', []))


@app.route('/api/managers')
def get_managers():
    """Get hiring manager workload"""
    data = get_cached_data()
    return jsonify(data.get('managers', []))


@app.route('/api/trend')
def get_trend():
    """Get monthly trend"""
    data = get_cached_data()
    return jsonify(data.get('trend', []))


@app.route('/api/critical')
def get_critical():
    """Get critical requisitions"""
    data = get_cached_data()
    return jsonify(data.get('critical', []))


@app.route('/api/all')
def get_all():
    """Get all dashboard data in one call"""
    data = get_cached_data()
    return jsonify(data)


@app.route('/api/refresh')
def refresh_cache():
    """Force cache refresh"""
    global cache_timestamp
    cache_timestamp = None
    data = get_cached_data()
    return jsonify({'status': 'Cache refreshed', 'timestamp': datetime.now().isoformat()})


if __name__ == '__main__':
    print("=" * 80)
    print("JOB REQUISITION DASHBOARD API")
    print("=" * 80)
    print(f"Starting Flask server...")
    print(f"Dashboard URL: http://localhost:8008")
    print(f"API Base URL: http://localhost:8008/api")
    print(f"Cache Duration: {CACHE_DURATION} seconds")
    print("=" * 80)

    # Pre-load cache on startup
    print("\nPre-loading cache...")
    get_cached_data()

    app.run(host='0.0.0.0', port=8008, debug=True, use_reloader=False)
