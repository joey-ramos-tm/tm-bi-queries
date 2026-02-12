/*
Lead Source Analysis - Detail Level
Shows individual contact records with lead source, appointment, and sale information
Use this for detailed analysis and troubleshooting
*/

-- CTE 1: Get first lead source per contact/account
WITH FirstLeadSource AS (
    SELECT
        CONTACT_ID,
        LEAD_SRC_ID,
        LEAD_SRC_NM,
        LEAD_SRC_TXT,
        CREATE_TMS,
        ROW_NUMBER() OVER (
            PARTITION BY CONTACT_ID
            ORDER BY CREATE_TMS ASC
        ) AS rn
    FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]
    WHERE CONTACT_ID IS NOT NULL
),

-- CTE 2: Get first appointment per contact
FirstAppointment AS (
    SELECT
        CONTACT_ID,
        EVENT_ID,
        ACTVTY_DT,
        TYPE_CD,
        ROW_NUMBER() OVER (
            PARTITION BY CONTACT_ID
            ORDER BY ACTVTY_DT ASC
        ) AS rn
    FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
    WHERE APP_TYPE_HANDLE_CD IN ('appointment', 'In Person Tour', 'Virtual Tour', 'Virtual Appointment')
        AND CONTACT_ID IS NOT NULL
        AND ACTVTY_DT IS NOT NULL
),

-- CTE 3: Get sale information per contact
FirstSale AS (
    SELECT
        c.CONTACT_ID,
        sd.QuoteReferenceName AS SALE_ID,
        sd.ApprovalDate AS SALE_DATE,
        sd.NetSalesPriceAmount AS SALE_AMOUNT,
        ROW_NUMBER() OVER (
            PARTITION BY c.CONTACT_ID
            ORDER BY sd.ApprovalDate ASC
        ) AS rn
    FROM [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail] sd
    INNER JOIN [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[CONTACT] c
        ON sd.AccountId = c.ACCT_ID
    WHERE c.CONTACT_ID IS NOT NULL
        AND sd.ApprovalDate IS NOT NULL
)

-- Detail level output with all contacts and their journey
SELECT
    fls.CONTACT_ID,
    fls.LEAD_SRC_ID,
    fls.LEAD_SRC_NM,
    fls.LEAD_SRC_TXT,
    fls.CREATE_TMS AS Lead_Source_Date,

    -- Appointment information
    fa.EVENT_ID AS First_Appointment_ID,
    fa.ACTVTY_DT AS First_Appointment_Date,
    fa.TYPE_CD AS Appointment_Type,

    -- Sale information
    fs.SALE_ID AS First_Sale_ID,
    fs.SALE_DATE AS First_Sale_Date,
    fs.SALE_AMOUNT,

    -- Days calculations
    DATEDIFF(DAY, fls.CREATE_TMS, fa.ACTVTY_DT) AS Days_LeadSource_To_Appointment,
    DATEDIFF(DAY, fa.ACTVTY_DT, fs.SALE_DATE) AS Days_Appointment_To_Sale,
    DATEDIFF(DAY, fls.CREATE_TMS, fs.SALE_DATE) AS Days_LeadSource_To_Sale,

    -- Journey stage classification
    CASE
        WHEN fs.SALE_DATE IS NOT NULL THEN 'Converted to Sale'
        WHEN fa.ACTVTY_DT IS NOT NULL THEN 'Had Appointment - No Sale'
        ELSE 'Lead Only - No Appointment'
    END AS Journey_Stage,

    -- Flags for filtering
    CASE WHEN fa.ACTVTY_DT IS NOT NULL THEN 1 ELSE 0 END AS Had_Appointment,
    CASE WHEN fs.SALE_DATE IS NOT NULL THEN 1 ELSE 0 END AS Had_Sale

FROM FirstLeadSource fls
LEFT JOIN FirstAppointment fa
    ON fls.CONTACT_ID = fa.CONTACT_ID
    AND fa.rn = 1
LEFT JOIN FirstSale fs
    ON fls.CONTACT_ID = fs.CONTACT_ID
    AND fs.rn = 1
WHERE fls.rn = 1  -- Only first lead source per contact

-- Optional: Add date range filter for recent data
-- AND fls.CREATE_TMS >= DATEADD(MONTH, -12, GETDATE())

ORDER BY fls.CREATE_TMS DESC;

/*
USAGE:
- Use this query to see individual contact journeys
- Export to Excel for detailed analysis
- Filter by specific lead sources for deep dives
- Useful for validating aggregated metrics
*/
