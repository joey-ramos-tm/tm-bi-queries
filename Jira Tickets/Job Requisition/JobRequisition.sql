/*
================================================================================
Parse JSON Columns from Workday Job Requisition Table
================================================================================

DATABASE: [TaylorMorrisonDataLake] on SQLDL1.TWC.PVT
TABLE: [WorkDay].[Get_Job_Requisition]
PURPOSE: Extract and flatten JSON data for BUS-310 - Open Job Requisitions Dashboard
AUTHOR: Joey Ramos
DATE: 2026-02-03
*/

SELECT
    -- Primary Key
    Requisition,
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

    ---- Parse Recruiter
    --JSON_VALUE(Recruiter_Reference, '$[1]."#text"') AS Recruiter_Name,
    --JSON_VALUE(Recruiter_Reference, '$[0]."#text"') AS Recruiter_WID,

    ---- Parse Hiring Manager
    --JSON_VALUE(Hiring_Manager_Reference, '$[1]."#text"') AS Hiring_Manager_Name,
    --JSON_VALUE(Hiring_Manager_Reference, '$[0]."#text"') AS Hiring_Manager_WID,

    -- Parse Requisition Reason (CRITICAL FIELD - Pete was waiting for this!)
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Requisition_Reason,
    JSON_VALUE(RequisitionReason, '$[0]."#text"') AS Requisition_Reason_WID

    -- Date Fields (likely already in proper format)
    --Job_Posting_Start_Date,
    --Job_Posting_End_Date,
    --Scheduled_Opening_Date,
    --Target_Completion_Date,

    -- Numeric Fields
    --CAST(Number_of_Openings AS INT) AS Number_of_Openings,
    --CAST(Referral_Bonus_Amount AS DECIMAL(10,2)) AS Referral_Bonus_Amount,

    ---- Additional Information (may be JSON or text)
    --Additional_Information,

    ---- Calculated Fields for Dashboard
    --CASE
    --    WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    --    THEN DATEDIFF(DAY, CAST(Job_Posting_Start_Date AS DATE), GETDATE())
    --    ELSE NULL
    --END AS Days_Open,

    --CASE
    --    WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    --        AND DATEDIFF(DAY, CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 60
    --    THEN 'Critical - 60+ Days'
    --    WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    --        AND DATEDIFF(DAY, CAST(Job_Posting_Start_Date AS DATE), GETDATE()) > 30
    --    THEN 'Warning - 30+ Days'
    --    WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    --    THEN 'Active - Under 30 Days'
    --    ELSE 'Not Open'
    --END AS Aging_Status

FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition] WHERE Requisition = 'R0013980'

-- Filter for recent requisitions (optional)
-- WHERE Job_Posting_Start_Date >= DATEADD(YEAR, -1, GETDATE())

ORDER BY Recruiting_Start_Date DESC
