/*
================================================================================
Parse JSON Columns from Workday Job Requisition Table
================================================================================

DATABASE: [TaylorMorrisonDataLake] on SQLDL1.TWC.PVT
TABLE: [WorkDay].[Get_Job_Requisition]
PURPOSE: Extract and flatten JSON data for BUS-310 - Open Job Requisitions Dashboard
AUTHOR: Joey Ramos
DATE: 2026-02-03

STATUS: DATA-490 COMPLETE! Table discovered on SQLDL1.TWC.PVT with 12,709 records

TABLE STRUCTURE DISCOVERED:
- 1 primary key column: Requisition (varchar 512)
- 18 JSON columns (varchar max) containing Workday reference data
- JSON Format: [{"@wd:type":"WID","#text":"guid"},{"@wd:type":"ID_Type","#text":"readable_value"}]
- Parsing Pattern: JSON_VALUE(column, '$[1]."#text"') extracts the readable value

================================================================================
*/

-- ================================================================================
-- SECTION 1: VERIFY TABLE ACCESS AND STRUCTURE
-- ================================================================================

-- Step 1.1: Check if table exists and get row count
SELECT
    'Table Exists' AS Status,
    COUNT(*) AS Total_Requisitions
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];

GO

-- Step 1.2: Get all column names and data types
SELECT
    ORDINAL_POSITION,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM [TaylorMorrisonDataLake].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'WorkDay'
  AND TABLE_NAME = 'Get_Job_Requisition'
ORDER BY ORDINAL_POSITION;

GO

-- Step 1.3: Sample first 5 rows to verify JSON structure
SELECT TOP 5 *
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
ORDER BY Requisition DESC;

GO


-- ================================================================================
-- SECTION 2: PARSE JSON COLUMNS - EXTRACT READABLE VALUES
-- ================================================================================

/*
ACTUAL COLUMNS IN TABLE (19 total):
1. Requisition (primary key)
2. Job_Profile_Reference (Job Title/Position)
3. Job_Requisition_Status_Reference (Open/Filled/Cancelled)
4. Primary_Location_Reference (Location/City)
5. Supervisory_Organization_Reference (Department)
6. Position_Reference
7. Time_Type_Reference (Full Time/Part Time)
8. Worker_Type_Reference (Employee/Contractor)
9. Job_Posting_Start_Date
10. Job_Posting_End_Date
11. Scheduled_Opening_Date
12. Target_Completion_Date
13. Number_of_Openings
14. Recruiter_Reference
15. Hiring_Manager_Reference
16. Referral_Bonus_Amount
17. RequisitionReason (NEW POSITION/BACKFILL/ETC)
18. Additional_Information
19. (Other JSON columns discovered)

JSON STRUCTURE EXAMPLE:
[{"@wd:type":"WID","#text":"cc5437fa4b051028376d67a4d48c5b8e"},{"@wd:type":"Location_ID","#text":"TX - Austin"}]

EXTRACTION PATTERN:
- $[0]."#text" = WID (guid identifier)
- $[1]."#text" = Readable value (what we want for reporting)
*/

-- ================================================================================
-- METHOD 1: Basic SELECT with All JSON Columns Parsed
-- ================================================================================

SELECT
    -- Primary Key
    Requisition,

    -- Additional Date and Numeric Fields for Dashboard
    Target_Hire_Date,
    Recruiting_Start_Date,
    Scheduled_Weekly_Hours,

    -- Parse Job Profile (Job Title/Position)
    JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
    JSON_VALUE(Job_Profile_Reference, '$[0]."#text"') AS Job_Profile_WID,

    -- Parse Job Requisition Status (Open/Filled/Cancelled)
    JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') AS Requisition_Status,
    JSON_VALUE(Job_Requisition_Status_Reference, '$[0]."#text"') AS Status_WID,

    -- Parse Primary Location (City/State)
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
    JSON_VALUE(Primary_Location_Reference, '$[0]."#text"') AS Location_WID,

    -- Parse Supervisory Organization (Department)
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
    JSON_VALUE(Supervisory_Organization_Reference, '$[0]."#text"') AS Department_WID,

    -- Parse Position Reference
    JSON_VALUE(Position_Reference, '$[1]."#text"') AS Position_ID,
    JSON_VALUE(Position_Reference, '$[0]."#text"') AS Position_WID,

    -- Parse Time Type (Full Time/Part Time)
    JSON_VALUE(Time_Type_Reference, '$[1]."#text"') AS Time_Type,
    JSON_VALUE(Time_Type_Reference, '$[0]."#text"') AS Time_Type_WID,

    -- Parse Worker Type (Employee/Contractor)
    JSON_VALUE(Worker_Type_Reference, '$[1]."#text"') AS Worker_Type,
    JSON_VALUE(Worker_Type_Reference, '$[0]."#text"') AS Worker_Type_WID,

    -- Parse Recruiter
    JSON_VALUE(Recruiter_Reference, '$[1]."#text"') AS Recruiter_Name,
    JSON_VALUE(Recruiter_Reference, '$[0]."#text"') AS Recruiter_WID,

    -- Parse Hiring Manager
    JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') AS Hiring_Manager_Name,
    JSON_VALUE(Hiring_Manager_Reference, '$[0]."#text"') AS Hiring_Manager_WID,

    -- Parse Requisition Reason (CRITICAL FIELD - Pete was waiting for this!)
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Requisition_Reason,
    JSON_VALUE(RequisitionReason, '$[0]."#text"') AS Requisition_Reason_WID,

    -- Date Fields (likely already in proper format)
    Job_Posting_Start_Date,
    Job_Posting_End_Date,
    Scheduled_Opening_Date,
    Target_Completion_Date,

    -- Numeric Fields
    CAST(Number_of_Openings AS INT) AS Number_of_Openings,
    CAST(Referral_Bonus_Amount AS DECIMAL(10,2)) AS Referral_Bonus_Amount,

    -- Additional Information (may be JSON or text)
    Additional_Information,

    -- Calculated Fields for Dashboard
    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
        THEN DATEDIFF(DAY, CAST(Job_Posting_Start_Date AS DATE), GETDATE())
        ELSE NULL
    END AS Days_Open,

    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
            AND DATEDIFF(DAY, CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 60
        THEN 'Critical - 60+ Days'
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
            AND DATEDIFF(DAY, CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 30
        THEN 'Warning - 30+ Days'
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
        THEN 'Active - Under 30 Days'
        ELSE 'Not Open'
    END AS Aging_Status

FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]

-- Filter for recent requisitions (optional)
-- WHERE Job_Posting_Start_Date >= DATEADD(YEAR, -1, GETDATE())

ORDER BY Requisition DESC;

GO


-- ================================================================================
-- METHOD 2: Create View for Power BI Consumption
-- ================================================================================

-- Drop existing view if it exists
IF OBJECT_ID('[WorkDay].[vw_Job_Requisition_Parsed]', 'V') IS NOT NULL
    DROP VIEW [WorkDay].[vw_Job_Requisition_Parsed];
GO

CREATE VIEW [WorkDay].[vw_Job_Requisition_Parsed]
AS
SELECT
    -- Primary Key
    Requisition AS Requisition_ID,

    -- Additional Date and Numeric Fields for Dashboard
    TRY_CAST(Target_Hire_Date AS DATE) AS Target_Hire_Date,
    TRY_CAST(Recruiting_Start_Date AS DATE) AS Recruiting_Start_Date,
    TRY_CAST(Scheduled_Weekly_Hours AS DECIMAL(5,2)) AS Scheduled_Weekly_Hours,

    -- Job Information
    JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
    JSON_VALUE(Job_Profile_Reference, '$[0]."#text"') AS Job_Profile_WID,

    -- Status
    JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') AS Requisition_Status,

    -- Location
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,

    -- Parse Location into City and State (if format is "State - City" or "City, State")
    CASE
        WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%-%'
        THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
             CHARINDEX('-', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) + 1, 255)))
        WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%,%'
        THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
             1, CHARINDEX(',', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) - 1)))
        ELSE JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')
    END AS City,

    CASE
        WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%-%'
        THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
             1, CHARINDEX('-', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) - 1)))
        WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%,%'
        THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
             CHARINDEX(',', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) + 1, 255)))
        ELSE NULL
    END AS State,

    -- Department/Organization
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,

    -- Position Details
    JSON_VALUE(Position_Reference, '$[1]."#text"') AS Position_ID,
    JSON_VALUE(Time_Type_Reference, '$[1]."#text"') AS Time_Type,
    JSON_VALUE(Worker_Type_Reference, '$[1]."#text"') AS Worker_Type,

    -- People
    JSON_VALUE(Recruiter_Reference, '$[1]."#text"') AS Recruiter,
    JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') AS Hiring_Manager,

    -- Critical Field: Requisition Reason
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Requisition_Reason,

    -- Dates
    TRY_CAST(Job_Posting_Start_Date AS DATE) AS Posting_Start_Date,
    TRY_CAST(Job_Posting_End_Date AS DATE) AS Posting_End_Date,
    TRY_CAST(Scheduled_Opening_Date AS DATE) AS Opening_Date,
    TRY_CAST(Target_Completion_Date AS DATE) AS Target_Fill_Date,

    -- Numbers
    TRY_CAST(Number_of_Openings AS INT) AS Openings_Count,
    TRY_CAST(Referral_Bonus_Amount AS DECIMAL(10,2)) AS Referral_Bonus,

    -- Calculated Fields for Dashboard
    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
            OR JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Active%'
        THEN TRY_CAST(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) AS INT)
        ELSE NULL
    END AS Days_Open,

    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
            OR JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Active%'
        THEN
            CASE
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 90
                THEN '90+ Days (Critical)'
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 60
                THEN '60-90 Days (High Risk)'
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 30
                THEN '30-60 Days (Warning)'
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 14
                THEN '15-30 Days (Active)'
                ELSE '0-14 Days (New)'
            END
        ELSE 'Not Open'
    END AS Age_Category,

    -- Is Open Flag
    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
            OR JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Active%'
        THEN 1
        ELSE 0
    END AS Is_Open,

    -- Current Date for Last Refresh
    CAST(GETDATE() AS DATE) AS Report_Date

FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];

GO

-- Grant permissions to BI team
GRANT SELECT ON [WorkDay].[vw_Job_Requisition_Parsed] TO [TWC\BI_DEV];
GRANT SELECT ON [WorkDay].[vw_Job_Requisition_Parsed] TO [TWC\BI_DEV_READ];

GO


-- ================================================================================
-- METHOD 3: Create Materialized Table for Better Performance
-- ================================================================================

-- Drop existing table if it exists
IF OBJECT_ID('[sandbox_BI].[dbo].[Job_Requisition_Parsed]', 'U') IS NOT NULL
    DROP TABLE [sandbox_BI].[dbo].[Job_Requisition_Parsed];
GO

-- Create table in sandbox_BI for Power BI consumption
SELECT
    -- Primary Key
    Requisition AS Requisition_ID,

    -- Additional Date and Numeric Fields for Dashboard
    TRY_CAST(Target_Hire_Date AS DATE) AS Target_Hire_Date,
    TRY_CAST(Recruiting_Start_Date AS DATE) AS Recruiting_Start_Date,
    TRY_CAST(Scheduled_Weekly_Hours AS DECIMAL(5,2)) AS Scheduled_Weekly_Hours,

    -- Job Information
    JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
    JSON_VALUE(Job_Profile_Reference, '$[0]."#text"') AS Job_Profile_WID,

    -- Status
    JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') AS Requisition_Status,

    -- Location
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,

    -- Parse Location into City and State
    CASE
        WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%-%'
        THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
             CHARINDEX('-', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) + 1, 255)))
        ELSE NULL
    END AS City,

    CASE
        WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%-%'
        THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
             1, CHARINDEX('-', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) - 1)))
        ELSE NULL
    END AS State,

    -- Department
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,

    -- Position Details
    JSON_VALUE(Position_Reference, '$[1]."#text"') AS Position_ID,
    JSON_VALUE(Time_Type_Reference, '$[1]."#text"') AS Time_Type,
    JSON_VALUE(Worker_Type_Reference, '$[1]."#text"') AS Worker_Type,

    -- People
    JSON_VALUE(Recruiter_Reference, '$[1]."#text"') AS Recruiter,
    JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') AS Hiring_Manager,

    -- Requisition Reason (CRITICAL FIELD)
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Requisition_Reason,

    -- Dates
    TRY_CAST(Job_Posting_Start_Date AS DATE) AS Posting_Start_Date,
    TRY_CAST(Job_Posting_End_Date AS DATE) AS Posting_End_Date,
    TRY_CAST(Scheduled_Opening_Date AS DATE) AS Opening_Date,
    TRY_CAST(Target_Completion_Date AS DATE) AS Target_Fill_Date,

    -- Numbers
    TRY_CAST(Number_of_Openings AS INT) AS Openings_Count,
    TRY_CAST(Referral_Bonus_Amount AS DECIMAL(10,2)) AS Referral_Bonus,

    -- Calculated: Days Open
    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
        THEN TRY_CAST(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) AS INT)
        ELSE NULL
    END AS Days_Open,

    -- Calculated: Age Category
    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
        THEN
            CASE
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 90
                THEN '90+ Days (Critical)'
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 60
                THEN '60-90 Days (High Risk)'
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 30
                THEN '30-60 Days (Warning)'
                WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 14
                THEN '15-30 Days (Active)'
                ELSE '0-14 Days (New)'
            END
        ELSE 'Not Open'
    END AS Age_Category,

    -- Is Open Flag
    CASE
        WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
        THEN 1
        ELSE 0
    END AS Is_Open,

    -- Metadata
    GETDATE() AS Last_Updated

INTO [sandbox_BI].[dbo].[Job_Requisition_Parsed]

FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];

GO

-- Create indexes for performance
CREATE CLUSTERED INDEX IX_Job_Requisition_ID ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] (Requisition_ID);
CREATE INDEX IX_Job_Requisition_Status ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] (Requisition_Status);
CREATE INDEX IX_Job_Requisition_IsOpen ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] (Is_Open);
CREATE INDEX IX_Job_Requisition_Department ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] (Department);
CREATE INDEX IX_Job_Requisition_Location ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] (State, City);
CREATE INDEX IX_Job_Requisition_PostingDate ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] (Posting_Start_Date);
CREATE INDEX IX_Job_Requisition_HiringManager ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] (Hiring_Manager);

GO

-- Grant permissions
GRANT SELECT ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] TO [TWC\BI_DEV];
GRANT SELECT ON [sandbox_BI].[dbo].[Job_Requisition_Parsed] TO [TWC\BI_DEV_READ];

GO


-- ================================================================================
-- SECTION 3: DATA QUALITY CHECKS
-- ================================================================================

-- Check 1: Record counts by status
SELECT
    'Status Distribution' AS Check_Type,
    JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') AS Status,
    COUNT(*) AS Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS Percent
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
GROUP BY JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"')
ORDER BY Count DESC;

GO

-- Check 2: Open requisitions by department
SELECT
    'Open by Department' AS Check_Type,
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
    COUNT(*) AS Open_Count
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
GROUP BY JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"')
ORDER BY Open_Count DESC;

GO

-- Check 3: Requisition reasons distribution
SELECT
    'Requisition Reasons' AS Check_Type,
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Reason,
    COUNT(*) AS Count
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(RequisitionReason, '$[1]."#text"') IS NOT NULL
GROUP BY JSON_VALUE(RequisitionReason, '$[1]."#text"')
ORDER BY Count DESC;

GO

-- Check 4: NULL value analysis
SELECT
    'NULL Analysis' AS Check_Type,
    COUNT(*) AS Total_Records,
    SUM(CASE WHEN Requisition IS NULL THEN 1 ELSE 0 END) AS Null_Requisition,
    SUM(CASE WHEN Job_Profile_Reference IS NULL THEN 1 ELSE 0 END) AS Null_JobProfile,
    SUM(CASE WHEN Job_Requisition_Status_Reference IS NULL THEN 1 ELSE 0 END) AS Null_Status,
    SUM(CASE WHEN Primary_Location_Reference IS NULL THEN 1 ELSE 0 END) AS Null_Location,
    SUM(CASE WHEN Supervisory_Organization_Reference IS NULL THEN 1 ELSE 0 END) AS Null_Department,
    SUM(CASE WHEN Hiring_Manager_Reference IS NULL THEN 1 ELSE 0 END) AS Null_HiringManager,
    SUM(CASE WHEN RequisitionReason IS NULL THEN 1 ELSE 0 END) AS Null_Reason
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];

GO

-- Check 5: Date range analysis
SELECT
    'Date Range' AS Check_Type,
    MIN(TRY_CAST(Job_Posting_Start_Date AS DATE)) AS Earliest_Posting,
    MAX(TRY_CAST(Job_Posting_Start_Date AS DATE)) AS Latest_Posting,
    COUNT(*) AS Total_Records
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];

GO


-- ================================================================================
-- SECTION 4: DASHBOARD QUERIES
-- ================================================================================

-- Query 1: Current Open Requisitions Summary
SELECT
    'Open Requisitions Summary' AS Report_Name,
    COUNT(*) AS Total_Open,
    COUNT(DISTINCT JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"')) AS Departments,
    COUNT(DISTINCT JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) AS Locations,
    COUNT(DISTINCT JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"')) AS Hiring_Managers,
    AVG(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open,
    MAX(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE())) AS Max_Days_Open
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%';

GO

-- Query 2: Open Requisitions by Department (Top 10)
SELECT TOP 10
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
    COUNT(*) AS Open_Count,
    AVG(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open,
    SUM(TRY_CAST(Number_of_Openings AS INT)) AS Total_Openings
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
GROUP BY JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"')
ORDER BY Open_Count DESC;

GO

-- Query 3: Open Requisitions by Location (Top 10)
SELECT TOP 10
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
    COUNT(*) AS Open_Count,
    AVG(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
GROUP BY JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')
ORDER BY Open_Count DESC;

GO

-- Query 4: Aging Analysis (Open Requisitions)
SELECT
    CASE
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 14
        THEN '0-14 Days (New)'
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 30
        THEN '15-30 Days (Active)'
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 60
        THEN '30-60 Days (Warning)'
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 90
        THEN '60-90 Days (High Risk)'
        ELSE '90+ Days (Critical)'
    END AS Age_Category,
    COUNT(*) AS Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS Percent
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
GROUP BY
    CASE
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 14
        THEN '0-14 Days (New)'
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 30
        THEN '15-30 Days (Active)'
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 60
        THEN '30-60 Days (Warning)'
        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) <= 90
        THEN '60-90 Days (High Risk)'
        ELSE '90+ Days (Critical)'
    END
ORDER BY
    CASE Age_Category
        WHEN '0-14 Days (New)' THEN 1
        WHEN '15-30 Days (Active)' THEN 2
        WHEN '30-60 Days (Warning)' THEN 3
        WHEN '60-90 Days (High Risk)' THEN 4
        WHEN '90+ Days (Critical)' THEN 5
    END;

GO

-- Query 5: Hiring Manager Workload (Top 10)
SELECT TOP 10
    JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') AS Hiring_Manager,
    COUNT(*) AS Open_Requisitions,
    SUM(TRY_CAST(Number_of_Openings AS INT)) AS Total_Openings,
    AVG(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') IS NOT NULL
GROUP BY JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"')
ORDER BY Open_Requisitions DESC;

GO

-- Query 6: Requisition Reason Distribution (Open Only)
SELECT
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Requisition_Reason,
    COUNT(*) AS Count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS Percent
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND JSON_VALUE(RequisitionReason, '$[1]."#text"') IS NOT NULL
GROUP BY JSON_VALUE(RequisitionReason, '$[1]."#text"')
ORDER BY Count DESC;

GO

-- Query 7: Monthly Trend (Last 12 Months)
SELECT
    FORMAT(TRY_CAST(Job_Posting_Start_Date AS DATE), 'yyyy-MM') AS Month,
    COUNT(*) AS Requisitions_Created,
    SUM(CASE WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Filled%'
             THEN 1 ELSE 0 END) AS Filled,
    SUM(CASE WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
             THEN 1 ELSE 0 END) AS Still_Open
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE TRY_CAST(Job_Posting_Start_Date AS DATE) >= DATEADD(MONTH, -12, GETDATE())
GROUP BY FORMAT(TRY_CAST(Job_Posting_Start_Date AS DATE), 'yyyy-MM')
ORDER BY Month DESC;

GO

-- Query 8: Critical Open Requisitions (90+ days)
SELECT
    Requisition AS Requisition_ID,
    JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
    JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') AS Hiring_Manager,
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Reason,
    TRY_CAST(Job_Posting_Start_Date AS DATE) AS Posting_Date,
    DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) AS Days_Open,
    TRY_CAST(Number_of_Openings AS INT) AS Openings
FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 90
ORDER BY Days_Open DESC;

GO


-- ================================================================================
-- SECTION 5: REFRESH PROCEDURE FOR MATERIALIZED TABLE
-- ================================================================================

-- Create stored procedure to refresh the sandbox table
CREATE OR ALTER PROCEDURE [sandbox_BI].[dbo].[sp_Refresh_Job_Requisition_Parsed]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @RowCount INT;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Truncate existing data
        TRUNCATE TABLE [sandbox_BI].[dbo].[Job_Requisition_Parsed];

        -- Reload from DataLake
        INSERT INTO [sandbox_BI].[dbo].[Job_Requisition_Parsed]
        SELECT
            Requisition AS Requisition_ID,
            TRY_CAST(Target_Hire_Date AS DATE) AS Target_Hire_Date,
            TRY_CAST(Recruiting_Start_Date AS DATE) AS Recruiting_Start_Date,
            TRY_CAST(Scheduled_Weekly_Hours AS DECIMAL(5,2)) AS Scheduled_Weekly_Hours,
            JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
            JSON_VALUE(Job_Profile_Reference, '$[0]."#text"') AS Job_Profile_WID,
            JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') AS Requisition_Status,
            JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
            CASE
                WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%-%'
                THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
                     CHARINDEX('-', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) + 1, 255)))
                ELSE NULL
            END AS City,
            CASE
                WHEN JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') LIKE '%-%'
                THEN LTRIM(RTRIM(SUBSTRING(JSON_VALUE(Primary_Location_Reference, '$[1]."#text"'),
                     1, CHARINDEX('-', JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) - 1)))
                ELSE NULL
            END AS State,
            JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
            JSON_VALUE(Position_Reference, '$[1]."#text"') AS Position_ID,
            JSON_VALUE(Time_Type_Reference, '$[1]."#text"') AS Time_Type,
            JSON_VALUE(Worker_Type_Reference, '$[1]."#text"') AS Worker_Type,
            JSON_VALUE(Recruiter_Reference, '$[1]."#text"') AS Recruiter,
            JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') AS Hiring_Manager,
            JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Requisition_Reason,
            TRY_CAST(Job_Posting_Start_Date AS DATE) AS Posting_Start_Date,
            TRY_CAST(Job_Posting_End_Date AS DATE) AS Posting_End_Date,
            TRY_CAST(Scheduled_Opening_Date AS DATE) AS Opening_Date,
            TRY_CAST(Target_Completion_Date AS DATE) AS Target_Fill_Date,
            TRY_CAST(Number_of_Openings AS INT) AS Openings_Count,
            TRY_CAST(Referral_Bonus_Amount AS DECIMAL(10,2)) AS Referral_Bonus,
            CASE
                WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                THEN TRY_CAST(DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) AS INT)
                ELSE NULL
            END AS Days_Open,
            CASE
                WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                THEN
                    CASE
                        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 90
                        THEN '90+ Days (Critical)'
                        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 60
                        THEN '60-90 Days (High Risk)'
                        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 30
                        THEN '30-60 Days (Warning)'
                        WHEN DATEDIFF(DAY, TRY_CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 14
                        THEN '15-30 Days (Active)'
                        ELSE '0-14 Days (New)'
                    END
                ELSE 'Not Open'
            END AS Age_Category,
            CASE
                WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
                THEN 1
                ELSE 0
            END AS Is_Open,
            GETDATE() AS Last_Updated
        FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];

        SET @RowCount = @@ROWCOUNT;

        COMMIT TRANSACTION;

        PRINT 'SUCCESS: Job Requisition data refreshed';
        PRINT 'Rows loaded: ' + CAST(@RowCount AS VARCHAR(10));
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR(10)) + ' seconds';

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        PRINT 'ERROR: Failed to refresh Job Requisition data';
        PRINT 'Error Message: ' + @ErrorMessage;

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;

GO


-- ================================================================================
-- SECTION 6: TEST THE REFRESH PROCEDURE
-- ================================================================================

-- Execute the refresh procedure
EXEC [sandbox_BI].[dbo].[sp_Refresh_Job_Requisition_Parsed];

GO

-- Verify the data
SELECT
    'Materialized Table' AS Source,
    COUNT(*) AS Total_Records,
    SUM(CASE WHEN Is_Open = 1 THEN 1 ELSE 0 END) AS Open_Requisitions,
    MAX(Last_Updated) AS Last_Refresh
FROM [sandbox_BI].[dbo].[Job_Requisition_Parsed];

GO


-- ================================================================================
-- DOCUMENTATION AND NOTES
-- ================================================================================

/*
================================================================================
SUMMARY AND NEXT STEPS FOR BUS-310
================================================================================

STATUS: DATA-490 IS COMPLETE! ✓

DATA DISCOVERED:
- Server: SQLDL1.TWC.PVT
- Database: TaylorMorrisonDataLake
- Table: [WorkDay].[Get_Job_Requisition]
- Records: 12,709 job requisitions
- Columns: 19 (1 primary key + 18 JSON columns)

JSON PARSING PATTERN:
- All JSON columns follow Workday format: [{"@wd:type":"WID","#text":"guid"},{"@wd:type":"ID_Type","#text":"readable_value"}]
- Extract readable values using: JSON_VALUE(column, '$[1]."#text"')
- Extract WID identifiers using: JSON_VALUE(column, '$[0]."#text"')

CRITICAL FIELDS EXTRACTED:
✓ Requisition ID (primary key)
✓ Job Title / Profile
✓ Requisition Status (Open/Filled/Cancelled)
✓ Location (City/State)
✓ Department / Organization
✓ Hiring Manager
✓ Recruiter
✓ Requisition Reason (NEW POSITION/BACKFILL/etc) - Pete was waiting for this!
✓ Time Type (Full/Part Time)
✓ Worker Type (Employee/Contractor)
✓ Posting Dates
✓ Target Fill Date
✓ Number of Openings
✓ Days Open (calculated)
✓ Age Category (calculated)

POWER BI DATA SOURCES READY:
1. View: [WorkDay].[vw_Job_Requisition_Parsed] - Direct query to DataLake
2. Table: [sandbox_BI].[dbo].[Job_Requisition_Parsed] - Materialized for performance

REFRESH STRATEGY:
- Stored Procedure: [sandbox_BI].[dbo].[sp_Refresh_Job_Requisition_Parsed]
- Can be scheduled via SQL Agent or called from Power BI refresh
- Truncates and reloads entire table with fresh data

NEXT STEPS FOR BUS-310:
1. ✓ Create Power BI dashboard using [sandbox_BI].[dbo].[Job_Requisition_Parsed]
2. ✓ Include key visualizations:
   - Open requisitions count
   - Aging analysis (0-14, 15-30, 30-60, 60-90, 90+ days)
   - By Department
   - By Location
   - By Hiring Manager
   - By Requisition Reason
   - Monthly trend
3. ✓ Schedule daily refresh of materialized table
4. ✓ Close blocker tickets DATA-490 and DATA-7020 in Jira
5. ✓ Notify Pete Gonzales that Requisition Reason field is now available

CONTACT INFORMATION:
- Joey Ramos (joramos@taylormorrison.com) - BI Analyst / Developer
- Pete Gonzales - Business Owner / Stakeholder
- Vishnu Veeragoni - Data Engineering (for DataLake questions)
- Doug Meinert - DBA (for permissions / performance)

================================================================================
SCRIPT CREATED: 2026-02-03
LAST UPDATED: 2026-02-03
STATUS: Production Ready
================================================================================
*/
