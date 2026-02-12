/*
Table Validation Script
Run this first to verify table access and understand the data structure
*/

-- 1. Check Lead Source table structure and sample data
PRINT '=== LEAD SOURCE TABLE ===';
SELECT TOP 10
    CONTACT_ID,
    LEAD_SRC_ID,
    LEAD_SRC_NM,
    LEAD_SRC_TXT,
    CREATE_TMS
FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]
WHERE CONTACT_ID IS NOT NULL
ORDER BY CREATE_TMS DESC;

PRINT 'Lead Source Record Count:';
SELECT COUNT(*) AS Total_Records,
       COUNT(DISTINCT CONTACT_ID) AS Unique_Contacts,
       COUNT(DISTINCT LEAD_SRC_NM) AS Unique_Lead_Sources
FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC];

-- 2. Check Event/Appointment table structure
PRINT '=== EVENT/APPOINTMENT TABLE ===';
SELECT TOP 10
    CONTACT_ID,
    EVENT_ID,
    EVENT_DATE,
    EVENT_TYPE,
    APP_TYPE_HANDLE_CD
FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
WHERE APP_TYPE_HANDLE_CD = 'APP_TYPE_HANDLE_CD'
    AND CONTACT_ID IS NOT NULL
ORDER BY EVENT_DATE DESC;

-- Check distinct APP_TYPE_HANDLE_CD values to verify correct filter
PRINT 'Distinct APP_TYPE_HANDLE_CD values:';
SELECT DISTINCT APP_TYPE_HANDLE_CD, COUNT(*) AS Count
FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
GROUP BY APP_TYPE_HANDLE_CD
ORDER BY Count DESC;

-- 3. IMPORTANT: Identify the actual sales table
PRINT '=== SALES TABLE - NEEDS IDENTIFICATION ===';
PRINT 'Search for sales-related tables:';

-- List tables in SILVER_DB that might contain sales data
SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM [TaylorMorrisonDWH_Silver].INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%SALE%'
   OR TABLE_NAME LIKE '%CONTRACT%'
   OR TABLE_NAME LIKE '%ORDER%'
   OR TABLE_NAME LIKE '%TRANSACTION%'
ORDER BY TABLE_NAME;

-- 4. Check for contacts that exist in multiple tables
PRINT '=== DATA OVERLAP CHECK ===';
WITH LeadContacts AS (
    SELECT DISTINCT CONTACT_ID
    FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]
    WHERE CONTACT_ID IS NOT NULL
),
AppointmentContacts AS (
    SELECT DISTINCT CONTACT_ID
    FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
    WHERE APP_TYPE_HANDLE_CD = 'APP_TYPE_HANDLE_CD'
        AND CONTACT_ID IS NOT NULL
)
SELECT
    'Contacts with Lead Source' AS Category,
    COUNT(*) AS Count
FROM LeadContacts
UNION ALL
SELECT
    'Contacts with Appointments' AS Category,
    COUNT(*) AS Count
FROM AppointmentContacts
UNION ALL
SELECT
    'Contacts with Both' AS Category,
    COUNT(*) AS Count
FROM LeadContacts lc
INNER JOIN AppointmentContacts ac ON lc.CONTACT_ID = ac.CONTACT_ID;

-- 5. Sample journey for one contact
PRINT '=== SAMPLE CONTACT JOURNEY ===';
DECLARE @SampleContactID VARCHAR(50);

-- Get a contact that has both lead source and appointment
SELECT TOP 1 @SampleContactID = ls.CONTACT_ID
FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC] ls
INNER JOIN [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT] e
    ON ls.CONTACT_ID = e.CONTACT_ID
WHERE e.APP_TYPE_HANDLE_CD = 'APP_TYPE_HANDLE_CD'
    AND ls.CONTACT_ID IS NOT NULL;

PRINT 'Sample Contact ID: ' + ISNULL(@SampleContactID, 'None Found');

-- Show lead sources for sample contact
SELECT
    'Lead Source' AS Source_Type,
    CONTACT_ID,
    LEAD_SRC_NM AS Detail,
    CREATE_TMS AS Event_Date
FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]
WHERE CONTACT_ID = @SampleContactID
UNION ALL
-- Show appointments for sample contact
SELECT
    'Appointment' AS Source_Type,
    CONTACT_ID,
    EVENT_TYPE AS Detail,
    EVENT_DATE AS Event_Date
FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
WHERE CONTACT_ID = @SampleContactID
    AND APP_TYPE_HANDLE_CD = 'APP_TYPE_HANDLE_CD'
ORDER BY Event_Date;

/*
ACTION ITEMS AFTER RUNNING THIS SCRIPT:
1. Verify CONTACT_ID links correctly between tables
2. Note the actual values of APP_TYPE_HANDLE_CD (if different from placeholder)
3. Identify the correct sales table from the list returned
4. Check sales table structure and determine column names:
   - Contact/Account ID column
   - Sale Date column
   - Sale Amount column
5. Update the main analysis queries with correct table/column names
*/
