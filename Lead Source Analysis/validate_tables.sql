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
    ACTVTY_DT,
    TYPE_CD,
    APP_TYPE_HANDLE_CD
FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
WHERE APP_TYPE_HANDLE_CD IN ('appointment', 'In Person Tour', 'Virtual Tour', 'Virtual Appointment')
    AND CONTACT_ID IS NOT NULL
ORDER BY ACTVTY_DT DESC;

-- Check distinct APP_TYPE_HANDLE_CD values to verify correct filter
PRINT 'Distinct APP_TYPE_HANDLE_CD values:';
SELECT DISTINCT APP_TYPE_HANDLE_CD, COUNT(*) AS Count
FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
GROUP BY APP_TYPE_HANDLE_CD
ORDER BY Count DESC;

-- 3. Check Sales table structure
PRINT '=== SALES TABLE (SaleDetail) ===';
SELECT TOP 10
    AccountId,
    QuoteReferenceName,
    ApprovalDate,
    SaleDate,
    NetSalesPriceAmount,
    BuyerName
FROM [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail]
WHERE ApprovalDate IS NOT NULL
ORDER BY ApprovalDate DESC;

PRINT 'Sales Record Count:';
SELECT COUNT(*) AS Total_Sales,
       COUNT(DISTINCT AccountId) AS Unique_Accounts
FROM [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail]
WHERE ApprovalDate IS NOT NULL;

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
    WHERE APP_TYPE_HANDLE_CD IN ('appointment', 'In Person Tour', 'Virtual Tour', 'Virtual Appointment')
        AND CONTACT_ID IS NOT NULL
),
SaleContacts AS (
    SELECT DISTINCT AccountId AS CONTACT_ID
    FROM [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail]
    WHERE ApprovalDate IS NOT NULL
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
    'Contacts with Sales' AS Category,
    COUNT(*) AS Count
FROM SaleContacts
UNION ALL
SELECT
    'Contacts with Lead + Appointment' AS Category,
    COUNT(*) AS Count
FROM LeadContacts lc
INNER JOIN AppointmentContacts ac ON lc.CONTACT_ID = ac.CONTACT_ID
UNION ALL
SELECT
    'Contacts with Lead + Sale' AS Category,
    COUNT(*) AS Count
FROM LeadContacts lc
INNER JOIN SaleContacts sc ON lc.CONTACT_ID = sc.CONTACT_ID
UNION ALL
SELECT
    'Contacts with All Three' AS Category,
    COUNT(*) AS Count
FROM LeadContacts lc
INNER JOIN AppointmentContacts ac ON lc.CONTACT_ID = ac.CONTACT_ID
INNER JOIN SaleContacts sc ON lc.CONTACT_ID = sc.CONTACT_ID;

-- 5. Sample journey for one contact
PRINT '=== SAMPLE CONTACT JOURNEY ===';
DECLARE @SampleContactID VARCHAR(50);

-- Get a contact that has lead source, appointment, and sale
SELECT TOP 1 @SampleContactID = ls.CONTACT_ID
FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC] ls
INNER JOIN [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT] e
    ON ls.CONTACT_ID = e.CONTACT_ID
INNER JOIN [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail] s
    ON ls.CONTACT_ID = s.AccountId
WHERE e.APP_TYPE_HANDLE_CD IN ('appointment', 'In Person Tour', 'Virtual Tour', 'Virtual Appointment')
    AND s.ApprovalDate IS NOT NULL
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
    TYPE_CD AS Detail,
    ACTVTY_DT AS Event_Date
FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
WHERE CONTACT_ID = @SampleContactID
    AND APP_TYPE_HANDLE_CD IN ('appointment', 'In Person Tour', 'Virtual Tour', 'Virtual Appointment')
UNION ALL
-- Show sales for sample contact
SELECT
    'Sale' AS Source_Type,
    AccountId AS CONTACT_ID,
    QuoteReferenceName AS Detail,
    ApprovalDate AS Event_Date
FROM [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail]
WHERE AccountId = @SampleContactID
    AND ApprovalDate IS NOT NULL
ORDER BY Event_Date;

/*
VALIDATION RESULTS:
1. LEAD_SRC table uses CONTACT_ID for linking
2. EVENT table uses CONTACT_ID and ACTVTY_DT for appointment dates
3. SaleDetail table uses AccountId for linking and ApprovalDate IS NOT NULL identifies sales
4. APP_TYPE_HANDLE_CD values include: appointment, In Person Tour, Virtual Tour, Virtual Appointment
5. All three tables are now properly configured in the analysis queries
*/
