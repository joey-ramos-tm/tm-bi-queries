-- =====================================================
-- Enhanced Job Requisition Query
-- Combines Get_Job_Requisition with related WorkDay tables
-- to get complete job and candidate information
-- =====================================================
-- Created: 2026-02-06
-- Source Tables Found: 26 WorkDay tables analyzed
-- Key Additional Tables:
--   - Candidate_Stage (for candidate status/process)
--   - JobFamilyGroup (for job family hierarchy)
--   - RPT_Active_Directory_Current_Row (for manager and division)
-- =====================================================

-- =====================================================
-- VERSION 1: Basic Enhanced Query
-- Includes all requested fields with Get_Job_Requisition
-- =====================================================

SELECT
    -- Base Requisition Info
    jr.Requisition,
    jr.Recruiting_Start_Date,
    jr.Target_Hire_Date,
    DATEDIFF(DAY, TRY_CAST(jr.Recruiting_Start_Date AS DATE), GETDATE()) AS Days_Open,

    -- Job Profile Information (from Get_Job_Requisition JSON fields)
    JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') AS Job_Profile_ID,

    -- Location Information
    JSON_VALUE(jr.Primary_Location_Reference, '$[1]."#text"') AS Location,

    -- Department/Organization
    JSON_VALUE(jr.Supervisory_Organization_Reference, '$[1]."#text"') AS Department,

    -- Time Type (Full Time vs Part Time)
    JSON_VALUE(jr.Time_Type_Reference, '$[1]."#text"') AS Time_Type,
    jr.Scheduled_Weekly_Hours,

    -- Status
    JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') AS Requisition_Status,

    -- Reason
    JSON_VALUE(jr.RequisitionReason, '$[1]."#text"') AS Requisition_Reason,

    -- Job Family Information (from JobFamilyGroup table)
    jfg.Job_Profile_Name,
    jfg.Job_Family_Name,
    jfg.Job_Family_Group_Name,

    -- Candidate Stage Information (from Candidate_Stage table)
    cs.CandidateName,
    cs.CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor AS Current_Candidate_Stage,
    cs.CF_ESI_Latest_Candidate_Stage_group_StageDate AS Stage_Date,
    cs.BusinessTitle,
    cs.JobTitle

FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition] jr

-- LEFT JOIN to JobFamilyGroup for Job Family hierarchy
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[JobFamilyGroup] jfg
    ON JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') = jfg.Job_Profile_Reference_Text

-- LEFT JOIN to Candidate_Stage for in-process status
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[Candidate_Stage] cs
    ON jr.Requisition = cs.JobReq

-- Filter for Open requisitions
WHERE JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'

ORDER BY Days_Open DESC;


-- =====================================================
-- VERSION 2: Enhanced Query with Manager and Division
-- Includes manager and region/division information
-- Note: This requires additional lookups that may need refinement
-- =====================================================

SELECT
    -- Base Requisition Info
    jr.Requisition,
    jr.Recruiting_Start_Date,
    jr.Target_Hire_Date,
    DATEDIFF(DAY, TRY_CAST(jr.Recruiting_Start_Date AS DATE), GETDATE()) AS Days_Open,

    -- Job Profile Information
    JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') AS Job_Profile_ID,
    jfg.Job_Profile_Name,

    -- Job Family Hierarchy
    jfg.Job_Family_Name,
    jfg.Job_Family_Group_Name,

    -- Location Information
    JSON_VALUE(jr.Primary_Location_Reference, '$[1]."#text"') AS Location,

    -- Department/Organization
    JSON_VALUE(jr.Supervisory_Organization_Reference, '$[1]."#text"') AS Department,

    -- Time Type (Full Time vs Part Time)
    JSON_VALUE(jr.Time_Type_Reference, '$[1]."#text"') AS Time_Type,
    jr.Scheduled_Weekly_Hours,

    -- Status
    JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') AS Requisition_Status,

    -- Reason
    JSON_VALUE(jr.RequisitionReason, '$[1]."#text"') AS Requisition_Reason,

    -- Candidate Information
    cs.CandidateName,
    cs.BusinessTitle,
    cs.JobTitle,

    -- Candidate Stage (In-Process Status)
    cs.CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor AS Current_Stage,
    cs.CF_ESI_Latest_Candidate_Stage_group_StageDate AS Stage_Date,

    -- Manager Information (if available from Staff_Changes)
    sc.HiringManagerProp_Descriptor AS Hiring_Manager,

    -- Division/Region (from Staff_Changes)
    sc.DivisionProp_Descriptor AS Division,

    -- Timestamps
    jr.DateCreated_TaylorMorrisonDatalake AS Date_Created,
    jr.DateModified_TaylorMorrisonDatalake AS Date_Modified

FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition] jr

-- Job Family hierarchy
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[JobFamilyGroup] jfg
    ON JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') = jfg.Job_Profile_Reference_Text

-- Candidate Stage information
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[Candidate_Stage] cs
    ON jr.Requisition = cs.JobReq

-- Staff Changes for Manager and Division (may need refinement on join key)
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[Staff_Changes] sc
    ON JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') = sc.JobProfProp_text

WHERE JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'

ORDER BY Days_Open DESC;


-- =====================================================
-- VERSION 3: Detailed Query with All Available Fields
-- Includes comprehensive information from all related tables
-- =====================================================

SELECT
    -- === REQUISITION BASICS ===
    jr.Requisition,
    jr.Recruiting_Start_Date,
    jr.Target_Hire_Date,
    DATEDIFF(DAY, TRY_CAST(jr.Recruiting_Start_Date AS DATE), GETDATE()) AS Days_Open,
    JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') AS Requisition_Status,
    JSON_VALUE(jr.RequisitionReason, '$[1]."#text"') AS Requisition_Reason,
    jr.Spotlight_Job,

    -- === JOB PROFILE ===
    JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') AS Job_Profile_ID,
    jfg.Job_Profile_Name AS Job_Profile,

    -- === JOB FAMILY HIERARCHY ===
    jfg.Job_Family_Name AS Job_Family,
    jfg.Job_Family_Group_Name AS Job_Family_Group,
    jfg.Job_Family_Group_ID,

    -- === TIME TYPE (FULL TIME VS PART TIME) ===
    JSON_VALUE(jr.Time_Type_Reference, '$[1]."#text"') AS Time_Type,
    jr.Scheduled_Weekly_Hours,

    -- === LOCATION ===
    JSON_VALUE(jr.Primary_Location_Reference, '$[1]."#text"') AS Primary_Location,
    JSON_VALUE(jr.Primary_Job_Posting_Location_Reference, '$[1]."#text"') AS Posting_Location,

    -- === DEPARTMENT/ORGANIZATION ===
    JSON_VALUE(jr.Supervisory_Organization_Reference, '$[1]."#text"') AS Department,

    -- === CANDIDATE INFORMATION ===
    cs.CandidateID,
    cs.CandidateName,
    cs.BusinessTitle AS Candidate_Business_Title,
    cs.JobTitle AS Candidate_Job_Title,

    -- === IN-PROCESS STATUS (CANDIDATE STAGE) ===
    cs.CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor AS Current_Stage,
    cs.CF_ESI_Latest_Candidate_Stage_group_CurrentStage_ID AS Stage_ID,
    cs.CF_ESI_Latest_Candidate_Stage_group_StageDate AS Stage_Date,

    -- === HIRING MANAGER ===
    sc.HiringManagerProp_Descriptor AS Hiring_Manager,
    sc.HiringManagerProp_ID AS Hiring_Manager_ID,

    -- === DIVISION/REGION ===
    sc.DivisionProp_Descriptor AS Division,
    sc.DivisionProp_ID AS Division_ID,

    -- === WORKER TYPE ===
    JSON_VALUE(jr.Worker_Type_Reference, '$[1]."#text"') AS Worker_Type,
    JSON_VALUE(jr.Position_Worker_Type_Reference, '$[1]."#text"') AS Position_Worker_Type,

    -- === POSITION INFO ===
    JSON_VALUE(jr.Position_Reference, '$[1]."#text"') AS Position_ID,

    -- === TIMESTAMPS ===
    jr.DateCreated_TaylorMorrisonDatalake AS Date_Created,
    jr.DateModified_TaylorMorrisonDatalake AS Date_Modified,
    cs.ROW_CREATE_TMS AS Candidate_Record_Created

FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition] jr

-- Job Family Group hierarchy
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[JobFamilyGroup] jfg
    ON JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') = jfg.Job_Profile_Reference_Text
    AND (jfg.Job_Family_Group_Inactive IS NULL OR jfg.Job_Family_Group_Inactive = '0')
    AND (jfg.Job_Family_Inactive IS NULL OR jfg.Job_Family_Inactive = '0')

-- Candidate Stage (for in-process status)
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[Candidate_Stage] cs
    ON jr.Requisition = cs.JobReq

-- Staff Changes (for manager and division)
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[Staff_Changes] sc
    ON JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') = sc.JobProfProp_text

WHERE JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'

ORDER BY Days_Open DESC;


-- =====================================================
-- REFERENCE: Key Tables and Their Purpose
-- =====================================================

/*
KEY TABLES DISCOVERED:

1. Get_Job_Requisition (Base table - 19 columns)
   - Contains requisition basics, status, dates
   - Has JSON fields for most relationships

2. Candidate_Stage (12 columns)
   - CandidateID, CandidateName
   - CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor (IN-PROCESS STATUS)
   - BusinessTitle, JobTitle
   - Links via: jr.Requisition = cs.JobReq

3. JobFamilyGroup (9 columns)
   - Job_Family_Name (JOB FAMILY)
   - Job_Family_Group_Name (JOB FAMILY GROUP)
   - Job_Profile_Name (JOB PROFILE)
   - Links via: Job_Profile_Reference_Text

4. Staff_Changes (13+ columns)
   - HiringManagerProp_Descriptor (HIRING MANAGER)
   - DivisionProp_Descriptor (DIVISION)
   - TimeTypeProp_Descriptor
   - JobFamilyGroupProp_Descriptor, JobFamilyProp_Descriptor

5. RPT_Active_Directory_Current_Row
   - manager, managerid (MANAGER)
   - division_Descriptor, region_Descriptor (DIVISION/REGION)
   - Job_Family_Group_Descriptor
   - bustitle (Business Title)

6. GetWorkers_JobPosition
   - Business_Title (BUSINESS TITLE)
   - Job_Profile_Name (JOB PROFILE)
   - Job_Family_Reference_text (JOB FAMILY)
   - Job_Group_Reference_text (JOB FAMILY GROUP)
   - Position_Time_Type_Reference_text (TIME TYPE)
   - Manager (MANAGER)

7. WorkerJobProfile
   - Similar to GetWorkers_JobPosition but different structure
   - Has all job family and profile information

CANDIDATE STAGES (from Candidate_Stage.CurrentStage_Descriptor):
Common values include:
- "Offer"
- "In Background Check"
- "Ready to Hire"
- "Interviewing"
- "Under Review"
- "Pending"
*/

-- =====================================================
-- SAMPLE QUERIES FOR SPECIFIC USE CASES
-- =====================================================

-- Query: Open requisitions with candidate stages (in-process status)
SELECT
    jr.Requisition,
    JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') AS Job_Profile_ID,
    jfg.Job_Profile_Name,
    cs.CandidateName,
    cs.CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor AS Stage,
    cs.CF_ESI_Latest_Candidate_Stage_group_StageDate AS Stage_Date,
    DATEDIFF(DAY, TRY_CAST(jr.Recruiting_Start_Date AS DATE), GETDATE()) AS Days_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition] jr
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[JobFamilyGroup] jfg
    ON JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') = jfg.Job_Profile_Reference_Text
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[Candidate_Stage] cs
    ON jr.Requisition = cs.JobReq
WHERE JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND cs.CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor IS NOT NULL
ORDER BY Stage_Date DESC;


-- Query: Distinct candidate stages (to see all possible values)
SELECT DISTINCT
    CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor AS Stage,
    COUNT(*) AS Count
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Candidate_Stage]
WHERE CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor IS NOT NULL
GROUP BY CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor
ORDER BY Count DESC;


-- Query: Job Family hierarchy with counts
SELECT
    jfg.Job_Family_Group_Name,
    jfg.Job_Family_Name,
    COUNT(DISTINCT jr.Requisition) AS Open_Requisitions
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition] jr
LEFT JOIN [TaylorMorrisonDWH_Bronze].[WorkDay].[JobFamilyGroup] jfg
    ON JSON_VALUE(jr.Job_Profile_Reference, '$[1]."#text"') = jfg.Job_Profile_Reference_Text
WHERE JSON_VALUE(jr.Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND jfg.Job_Family_Group_Name IS NOT NULL
GROUP BY jfg.Job_Family_Group_Name, jfg.Job_Family_Name
ORDER BY Open_Requisitions DESC;


-- =====================================================
-- NOTES AND RECOMMENDATIONS
-- =====================================================

/*
FIELD MAPPING SUMMARY:

✅ Job Title/Business Title
   - Found in: Candidate_Stage.BusinessTitle, Candidate_Stage.JobTitle
   - Also in: GetWorkers_JobPosition.Business_Title

✅ Job Profile
   - Found in: JobFamilyGroup.Job_Profile_Name
   - Source: Get_Job_Requisition.Job_Profile_Reference (JSON)

✅ Job Family
   - Found in: JobFamilyGroup.Job_Family_Name

✅ Job Family Group
   - Found in: JobFamilyGroup.Job_Family_Group_Name

✅ Time_Type (Full time vs Part Time)
   - Found in: Get_Job_Requisition.Time_Type_Reference (JSON)
   - Also: Get_Job_Requisition.Scheduled_Weekly_Hours (numeric)

✅ Hiring Manager
   - Found in: Staff_Changes.HiringManagerProp_Descriptor
   - Also in: GetWorkers_JobPosition.Manager
   - Also in: RPT_Active_Directory_Current_Row.manager

⚠️  Division (Region in Workday)
   - Found in: Staff_Changes.DivisionProp_Descriptor
   - Also in: RPT_Active_Directory_Current_Row.division_Descriptor, region_Descriptor
   - Note: May need additional logic to link to requisitions

✅ In process status (offer, background, ready to hire)
   - Found in: Candidate_Stage.CF_ESI_Latest_Candidate_Stage_group_CurrentStage_Descriptor
   - This is the primary field for candidate pipeline status

IMPORTANT CONSIDERATIONS:

1. JOIN KEYS: The joins in these queries are estimates based on field names.
   You may need to refine them based on your data relationships.

2. MANAGER INFORMATION: The Staff_Changes table join may need adjustment.
   Consider also using GetWorkers_JobPosition.Manager if you have
   a way to link requisitions to current workers/positions.

3. DIVISION/REGION: This may require joining through organization tables.
   RPT_Active_Directory_Current_Row has division but needs a worker ID.

4. CANDIDATE_STAGE: This table links directly to requisitions via JobReq.
   It provides the most reliable candidate status information.

5. TIME_TYPE: The JSON field contains an ID. You may want to create
   a lookup to translate to "Full Time" / "Part Time" labels.

6. JOB FAMILY: The JobFamilyGroup join works when Job_Profile_Reference
   matches Job_Profile_Reference_Text. Verify this in your data.

NEXT STEPS:

1. Test Version 1 (Basic Query) first to ensure joins work correctly
2. Examine the data to verify field values match expectations
3. Refine joins for Manager and Division based on your data structure
4. Add any additional filters or sorting as needed
5. Consider creating a VIEW for easy reuse

*/
