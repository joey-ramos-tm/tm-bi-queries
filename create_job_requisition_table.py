import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connection string for sandbox_BI on SQLDWH1
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER=SQLDWH1.TWC.PVT;"
    f"DATABASE=sandbox_BI;"
    f"Trusted_Connection=yes;"
)

# The query from the file
query = """
-- Drop table if exists
IF OBJECT_ID('[Demo].[JobRequisition]', 'U') IS NOT NULL
    DROP TABLE [Demo].[JobRequisition];

-- Create the table with the query
SELECT
    jr.*,
    jp.Job_Family_Name,
    jp.Job_Family_Inactive,
    jp.Job_Family_Group_ID,
    jp.Job_Family_Group_Name,
    jp.Job_Family_Group_Inactive,
    jp.wd_type,
    jp.wd_text,
    jp.Job_Profile_Name,
    ldp.AREA_NM,
    ldp.DIVISION,
    ldp.DEPT_NM,
    ldp.DEPT_ID
INTO [Demo].[JobRequisition]
FROM (
    SELECT
        Requisition,

        -- Date Fields (not JSON)
        CAST(Recruiting_Start_Date AS DATE) AS Recruiting_Start_Date,
        CAST(Target_Hire_Date AS DATE) AS Target_Hire_Date,
        DateCreated_TaylorMorrisonDatalake,
        DateModified_TaylorMorrisonDatalake,

        -- Numeric Fields (not JSON)
        Scheduled_Weekly_Hours,

        -- JSON Column 3: Supervisory_Organization_Reference (DEPARTMENT)
        JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department_ID,

        -- JSON Column 4: Position_Reference
        JSON_VALUE(Position_Reference, '$[1]."#text"') AS Position_ID,

        -- JSON Column 5: Time_Type_Reference (FULL TIME VS PART TIME)
        JSON_VALUE(Time_Type_Reference, '$[1]."#text"') AS Time_Type_ID,

        -- JSON Column 6: Primary_Job_Posting_Location_Reference
        JSON_VALUE(Primary_Job_Posting_Location_Reference, '$[1]."#text"') AS Posting_Location_ID,

        -- JSON Column 7: Primary_Location_Reference (LOCATION)
        JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location_ID,

        --JSON Column 8: Position_Worker_Type_Reference
        JSON_VALUE(Position_Worker_Type_Reference, '$[1]."#text"') AS Position_Worker_Type_ID,

        --JSON Column 9: Worker_Type_Reference
        JSON_VALUE(Worker_Type_Reference, '$[1]."#text"') AS Worker_Type_ID,

        --JSON Column 10: Job_Profile_Reference (JOB PROFILE)
        JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Profile_ID,

        --JSON Column 11: Job_Requisition_Status_Reference (STATUS)
        JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') AS Status_ID

    FROM [SQLDL1].[TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
    WHERE Recruiting_Start_Date >= '2025-01-01'
) jr

LEFT JOIN (
    SELECT
        Job_Family_Name,
        Job_Family_Inactive,
        REPLACE(Job_Family_Group_ID,' Group','') AS Job_Family_Group_ID,
        REPLACE(Job_Family_Group_Name,' Group','') AS Job_Family_Group_Name,
        Job_Family_Group_Inactive,
        wd_type,
        wd_text,
        JSON_VALUE(job.value, '$."wd:Job_Profile_Reference"."wd:ID"[1]."#text"') AS Job_Profile_ID,
        JSON_VALUE(job.value, '$."wd:Job_Profile_Name"') AS Job_Profile_Name
    FROM [SQLDL1].[TaylorMorrisonDWH_Bronze].[WorkDay].[JobFamilyGroup]
    CROSS APPLY OPENJSON(Job_Profile_Info_Data) AS job
    WHERE wd_type = 'Job_Family_ID'
) jp ON jr.Job_Profile_ID = jp.Job_Profile_ID

LEFT JOIN (
    SELECT
        CASE
            WHEN DEPT_ID = 'Build To Rent' THEN 'Build To Rent'
            ELSE AREA_NM
        END AS AREA_NM,
        CASE
            WHEN DEPT_ID = 'Build To Rent' THEN 'Build To Rent'
            WHEN AREA_NM = 'Mortgage' THEN 'Mortgage'
            ELSE REPLACE(DIVISION_NM,' Division','')
        END AS DIVISION,
        DEPT_NM,
        DEPT_ID
    FROM [TaylorMorrisonDWH_Gold].[PEOPLE_MGMT_VW].[PRSNEL_DETL]
    WHERE PRSNEL_ACTV_IND = 1
        AND DEPT_ID IS NOT NULL
    GROUP BY
        CASE
            WHEN DEPT_ID = 'Build To Rent' THEN 'Build To Rent'
            ELSE AREA_NM
        END,
        CASE
            WHEN DEPT_ID = 'Build To Rent' THEN 'Build To Rent'
            WHEN AREA_NM = 'Mortgage' THEN 'Mortgage'
            ELSE REPLACE(DIVISION_NM,' Division','')
        END,
        DEPT_NM,
        DEPT_ID
) ldp ON jr.Department_ID = ldp.DEPT_ID

WHERE jr.Job_Profile_ID NOT IN ('CONINT','LPM');
"""

try:
    print("Connecting to sandbox_BI database on SQLDWH1...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    print("Executing query to create JobRequisition table...")
    cursor.execute(query)
    conn.commit()

    print("✓ Table [sandbox_BI].[Demo].[JobRequisition] created successfully!")

    # Get row count
    cursor.execute("SELECT COUNT(*) FROM [Demo].[JobRequisition]")
    row_count = cursor.fetchone()[0]
    print(f"✓ Loaded {row_count} rows into the table")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
    raise
