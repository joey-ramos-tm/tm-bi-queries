/*
Parse JSON columns from PEOPLE_MGMT_VW.PRSNEL_DETL using SQL
For BUS-310 - Open Job Requisitions Dashboard (investigation)

This script uses SQL Server's JSON functions to parse the BUSINES_SITE_ADDR_TXT column
and extract address components into separate columns.

Author: Joey Ramos
Date: 2026-02-03
*/

-- =====================================================
-- METHOD 1: Simple Query with Parsed JSON
-- =====================================================

SELECT
    -- Original Personnel Fields
    PRSNEL_ID,
    PRSNEL_USER_ID,
    EMP_IND,
    PRSNEL_TYPE_TXT,
    AREA_NM,
    DIVISION_NM,
    DIVISION_CD,
    DEPT_NM,
    FORMAT_NM,
    PRSNEL_FIRST_NM,
    PRSNEL_MIDDLE_NM,
    PRSNEL_LAST_NM,
    MGR_PRSNEL_ID,
    MGR_NM,
    JOB_POSITN_ID,
    JOB_POSITN_TITLE_TXT,
    JOB_BUSINES_TITLE_TXT,
    JOB_POSITN_TIME_REFRNC_TXT,
    JOB_PROFL_NM,
    JOB_FAMLY_GROUP_NM,
    JOB_FAMLY_REFRNC_ID,
    JOB_FAMLY_NM,
    PRSNEL_ACTV_STATUS_DT,
    PRSNEL_ACTV_IND,
    PRSNEL_HIRE_DT,
    PRSNEL_CNTRCT_END_DT,
    PRMRY_TRMNT_CATG_REFRNC_ID,
    PRMRY_TRMNT_REASON_REFRNC_TYPE_TXT,
    PRMRY_TRMNT_REASON_REFRNC_ID,
    PRSNEL_TRMNT_DT,
    PRSNEL_TRMNT_IND,
    PRSNEL_REHIRE_IND,
    PRSNEL_NOT_RTRN_IND,
    PRSNEL_TRMNT_INVLNTRY_IND,
    PRSNEL_REGRET_TRMNT_IND,
    CMPNY_NM,
    EMAIL_ADDR_TXT,
    WORKER_TYPE,
    WORKER_STATUS_TXT,
    DEPT_ID,
    COST_CENTER_CD,
    COST_CENTER_NM,
    WORKDAY_ACCT_ACTV_IND,

    -- Parsed Address Fields from JSON
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Effective_Date"') AS Address_Effective_Date,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Address_Format_Type"') AS Address_Format,

    -- Formatted Address (replace &#xa; with space)
    REPLACE(
        JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Formatted_Address"'),
        '&#xa;',
        ' '
    ) AS Formatted_Address,

    -- Address Lines
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Address_Line_Data"."#text"') AS Address_Line_2,

    -- City/Municipality
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Municipality"') AS City,

    -- State/Region
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Descriptor"') AS State,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Reference"."wd:ID"."#text"') AS State_Code,

    -- Postal Code
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Postal_Code"') AS Postal_Code,

    -- Country
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Reference"."wd:ID"."#text"') AS Country_Code

FROM
    PEOPLE_MGMT_VW.PRSNEL_DETL
WHERE
    PRSNEL_ACTV_IND = 1  -- Active employees only
    OR (PRSNEL_TRMNT_IND = 1 AND PRSNEL_TRMNT_DT >= DATEADD(YEAR, -1, GETDATE()))  -- Or terminated in last year
ORDER BY
    PRSNEL_ID DESC;


-- =====================================================
-- METHOD 2: Create a View with Parsed JSON
-- =====================================================

/*
-- Uncomment to create a permanent view

CREATE OR ALTER VIEW PEOPLE_MGMT_VW.PRSNEL_DETL_WITH_ADDRESS
AS
SELECT
    -- Original Personnel Fields
    p.PRSNEL_ID,
    p.PRSNEL_USER_ID,
    p.EMP_IND,
    p.PRSNEL_TYPE_TXT,
    p.AREA_NM,
    p.DIVISION_NM,
    p.DIVISION_CD,
    p.DEPT_NM,
    p.FORMAT_NM,
    p.PRSNEL_FIRST_NM,
    p.PRSNEL_MIDDLE_NM,
    p.PRSNEL_LAST_NM,
    p.MGR_PRSNEL_ID,
    p.MGR_NM,
    p.JOB_POSITN_ID,
    p.JOB_POSITN_TITLE_TXT,
    p.JOB_BUSINES_TITLE_TXT,
    p.JOB_POSITN_TIME_REFRNC_TXT,
    p.JOB_PROFL_NM,
    p.JOB_FAMLY_GROUP_NM,
    p.JOB_FAMLY_REFRNC_ID,
    p.JOB_FAMLY_NM,
    p.PRSNEL_ACTV_STATUS_DT,
    p.PRSNEL_ACTV_IND,
    p.PRSNEL_HIRE_DT,
    p.PRSNEL_CNTRCT_END_DT,
    p.PRMRY_TRMNT_CATG_REFRNC_ID,
    p.PRMRY_TRMNT_REASON_REFRNC_TYPE_TXT,
    p.PRMRY_TRMNT_REASON_REFRNC_ID,
    p.PRSNEL_TRMNT_DT,
    p.PRSNEL_TRMNT_IND,
    p.PRSNEL_REHIRE_IND,
    p.PRSNEL_NOT_RTRN_IND,
    p.PRSNEL_TRMNT_INVLNTRY_IND,
    p.PRSNEL_REGRET_TRMNT_IND,
    p.CMPNY_NM,
    p.EMAIL_ADDR_TXT,
    p.WORKER_TYPE,
    p.WORKER_STATUS_TXT,
    p.DEPT_ID,
    p.COST_CENTER_CD,
    p.COST_CENTER_NM,
    p.WORKDAY_ACCT_ACTV_IND,

    -- Parsed Address Fields
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Effective_Date"') AS Address_Effective_Date,
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Address_Format_Type"') AS Address_Format,
    REPLACE(JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Formatted_Address"'), '&#xa;', ' ') AS Formatted_Address,
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."wd:Address_Line_Data"."#text"') AS Address_Line_2,
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."wd:Municipality"') AS City,
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Descriptor"') AS State,
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Reference"."wd:ID"."#text"') AS State_Code,
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."wd:Postal_Code"') AS Postal_Code,
    JSON_VALUE(p.BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Reference"."wd:ID"."#text"') AS Country_Code

FROM
    PEOPLE_MGMT_VW.PRSNEL_DETL AS p;

GO
*/


-- =====================================================
-- METHOD 3: Using OPENJSON for More Complex Parsing
-- =====================================================

-- This method handles multiple address lines if they exist as an array

SELECT
    p.PRSNEL_ID,
    p.FORMAT_NM,
    p.JOB_POSITN_TITLE_TXT,
    p.DEPT_NM,
    p.WORKER_STATUS_TXT,
    p.PRSNEL_ACTV_IND,

    -- Parse the JSON array
    addr.Effective_Date,
    addr.Address_Format,
    addr.Formatted_Address,
    addr.Municipality,
    addr.State,
    addr.State_Code,
    addr.Postal_Code,
    addr.Country_Code

FROM
    PEOPLE_MGMT_VW.PRSNEL_DETL AS p

    -- Use OPENJSON to parse the JSON array
    CROSS APPLY OPENJSON(p.BUSINES_SITE_ADDR_TXT, '$')
    WITH (
        Effective_Date VARCHAR(50) '$."@wd:Effective_Date"',
        Address_Format VARCHAR(50) '$."@wd:Address_Format_Type"',
        Formatted_Address NVARCHAR(500) '$."@wd:Formatted_Address"',
        Municipality VARCHAR(100) '$."wd:Municipality"',
        State VARCHAR(100) '$."wd:Country_Region_Descriptor"',
        State_Code VARCHAR(10) '$."wd:Country_Region_Reference"."wd:ID"."#text"',
        Postal_Code VARCHAR(20) '$."wd:Postal_Code"',
        Country_Code VARCHAR(10) '$."wd:Country_Reference"."wd:ID"."#text"'
    ) AS addr

WHERE
    p.PRSNEL_ACTV_IND = 1

ORDER BY
    p.PRSNEL_ID DESC;


-- =====================================================
-- METHOD 4: Summary Statistics on Parsed Data
-- =====================================================

-- Count employees by city and state
SELECT
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Municipality"') AS City,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Descriptor"') AS State,
    COUNT(*) AS Employee_Count
FROM
    PEOPLE_MGMT_VW.PRSNEL_DETL
WHERE
    PRSNEL_ACTV_IND = 1
GROUP BY
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Municipality"'),
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Descriptor"')
ORDER BY
    Employee_Count DESC;


-- =====================================================
-- METHOD 5: Find Recruiting/Talent Acquisition Staff
-- =====================================================

-- Find personnel involved in recruiting/hiring
SELECT
    PRSNEL_ID,
    FORMAT_NM,
    JOB_POSITN_TITLE_TXT,
    JOB_FAMLY_NM,
    DEPT_NM,
    EMAIL_ADDR_TXT,
    WORKER_STATUS_TXT,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Municipality"') AS City,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Descriptor"') AS State
FROM
    PEOPLE_MGMT_VW.PRSNEL_DETL
WHERE
    PRSNEL_ACTV_IND = 1
    AND (
        JOB_POSITN_TITLE_TXT LIKE '%Recruit%'
        OR JOB_POSITN_TITLE_TXT LIKE '%Talent%'
        OR JOB_POSITN_TITLE_TXT LIKE '%Staffing%'
        OR DEPT_NM LIKE '%Talent%'
        OR DEPT_NM LIKE '%Recruit%'
    )
ORDER BY
    JOB_POSITN_TITLE_TXT;


-- =====================================================
-- METHOD 6: Export to Table for Power BI
-- =====================================================

/*
-- Uncomment to create a permanent table with parsed data

DROP TABLE IF EXISTS WORK_DB.PRSNEL_DETL_PARSED;

SELECT
    -- Original Personnel Fields
    PRSNEL_ID,
    PRSNEL_USER_ID,
    EMP_IND,
    PRSNEL_TYPE_TXT,
    AREA_NM,
    DIVISION_NM,
    DIVISION_CD,
    DEPT_NM,
    FORMAT_NM,
    PRSNEL_FIRST_NM,
    PRSNEL_MIDDLE_NM,
    PRSNEL_LAST_NM,
    MGR_PRSNEL_ID,
    MGR_NM,
    JOB_POSITN_ID,
    JOB_POSITN_TITLE_TXT,
    JOB_BUSINES_TITLE_TXT,
    JOB_POSITN_TIME_REFRNC_TXT,
    JOB_PROFL_NM,
    JOB_FAMLY_GROUP_NM,
    JOB_FAMLY_REFRNC_ID,
    JOB_FAMLY_NM,
    PRSNEL_ACTV_STATUS_DT,
    PRSNEL_ACTV_IND,
    PRSNEL_HIRE_DT,
    PRSNEL_CNTRCT_END_DT,
    PRMRY_TRMNT_CATG_REFRNC_ID,
    PRMRY_TRMNT_REASON_REFRNC_TYPE_TXT,
    PRMRY_TRMNT_REASON_REFRNC_ID,
    PRSNEL_TRMNT_DT,
    PRSNEL_TRMNT_IND,
    PRSNEL_REHIRE_IND,
    PRSNEL_NOT_RTRN_IND,
    PRSNEL_TRMNT_INVLNTRY_IND,
    PRSNEL_REGRET_TRMNT_IND,
    CMPNY_NM,
    EMAIL_ADDR_TXT,
    WORKER_TYPE,
    WORKER_STATUS_TXT,
    DEPT_ID,
    COST_CENTER_CD,
    COST_CENTER_NM,
    WORKDAY_ACCT_ACTV_IND,

    -- Parsed Address Fields from JSON
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Effective_Date"') AS Address_Effective_Date,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Address_Format_Type"') AS Address_Format,
    REPLACE(JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."@wd:Formatted_Address"'), '&#xa;', ' ') AS Formatted_Address,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Address_Line_Data"."#text"') AS Address_Line_2,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Municipality"') AS City,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Descriptor"') AS State,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Region_Reference"."wd:ID"."#text"') AS State_Code,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Postal_Code"') AS Postal_Code,
    JSON_VALUE(BUSINES_SITE_ADDR_TXT, '$[0]."wd:Country_Reference"."wd:ID"."#text"') AS Country_Code

INTO
    WORK_DB.PRSNEL_DETL_PARSED

FROM
    PEOPLE_MGMT_VW.PRSNEL_DETL
WHERE
    PRSNEL_ACTV_IND = 1
    OR (PRSNEL_TRMNT_IND = 1 AND PRSNEL_TRMNT_DT >= DATEADD(YEAR, -1, GETDATE()));

-- Create index on commonly filtered columns
CREATE INDEX IX_PRSNEL_DETL_PARSED_Active ON WORK_DB.PRSNEL_DETL_PARSED (PRSNEL_ACTV_IND);
CREATE INDEX IX_PRSNEL_DETL_PARSED_Area ON WORK_DB.PRSNEL_DETL_PARSED (AREA_NM);
CREATE INDEX IX_PRSNEL_DETL_PARSED_Division ON WORK_DB.PRSNEL_DETL_PARSED (DIVISION_NM);
CREATE INDEX IX_PRSNEL_DETL_PARSED_JobFamily ON WORK_DB.PRSNEL_DETL_PARSED (JOB_FAMLY_NM);

-- Grant access to BI team
GRANT SELECT ON WORK_DB.PRSNEL_DETL_PARSED TO [TWC\BI_DEV];
GRANT SELECT ON WORK_DB.PRSNEL_DETL_PARSED TO [TWC\BI_DEV_READ];

SELECT 'Table created successfully!' AS Status, COUNT(*) AS Row_Count
FROM WORK_DB.PRSNEL_DETL_PARSED;
*/


-- =====================================================
-- NOTES AND DOCUMENTATION
-- =====================================================

/*
JSON Path Examples for BUSINES_SITE_ADDR_TXT:

The JSON structure is an array containing address objects. Example path:
$[0]                                  - First address in array
$[0]."@wd:Effective_Date"            - Effective date attribute
$[0]."wd:Municipality"                - City/Municipality element
$[0]."wd:Country_Region_Descriptor"  - State name
$[0]."wd:Postal_Code"                - ZIP/Postal code
$[0]."wd:Country_Region_Reference"."wd:ID"."#text" - State code (e.g., "US-AZ")

SQL Server JSON Functions Used:
- JSON_VALUE()  : Extract scalar values from JSON
- OPENJSON()    : Parse JSON into table format
- REPLACE()     : Clean up HTML entities like &#xa;

Performance Notes:
- JSON parsing in SQL is efficient but not indexed
- For repeated queries, consider creating a materialized view or table
- Use WHERE clauses before JSON parsing when possible

For BUS-310 Dashboard:
- This provides personnel/employee data
- Still need Job Requisition data from DATA-490
- Can use this data for a hiring trends dashboard as alternative
*/
