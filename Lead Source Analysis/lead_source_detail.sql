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
        LEAD_SRC_NAME,
        LEAD_SRC_TYPE,
        CREATED_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY CONTACT_ID
            ORDER BY CREATED_DATE ASC
        ) AS rn
    FROM [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]
    WHERE CONTACT_ID IS NOT NULL
),

-- CTE 2: Get first appointment per contact
FirstAppointment AS (
    SELECT
        CONTACT_ID,
        EVENT_ID,
        EVENT_DATE,
        EVENT_TYPE,
        ROW_NUMBER() OVER (
            PARTITION BY CONTACT_ID
            ORDER BY EVENT_DATE ASC
        ) AS rn
    FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT]
    WHERE APP_TYPE_HANDLE_CD = 'APP_TYPE_HANDLE_CD'
        AND CONTACT_ID IS NOT NULL
        AND EVENT_DATE IS NOT NULL
),

-- CTE 3: Get sale information per contact
FirstSale AS (
    SELECT
        CONTACT_ID,
        SALE_ID,
        SALE_DATE,
        SALE_AMOUNT,
        ROW_NUMBER() OVER (
            PARTITION BY CONTACT_ID
            ORDER BY SALE_DATE ASC
        ) AS rn
    FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[SALE]  -- Update with actual sales table
    WHERE CONTACT_ID IS NOT NULL
        AND SALE_DATE IS NOT NULL
)

-- Detail level output with all contacts and their journey
SELECT
    fls.CONTACT_ID,
    fls.LEAD_SRC_ID,
    fls.LEAD_SRC_NAME,
    fls.LEAD_SRC_TYPE,
    fls.CREATED_DATE AS Lead_Source_Date,

    -- Appointment information
    fa.EVENT_ID AS First_Appointment_ID,
    fa.EVENT_DATE AS First_Appointment_Date,
    fa.EVENT_TYPE AS Appointment_Type,

    -- Sale information
    fs.SALE_ID AS First_Sale_ID,
    fs.SALE_DATE AS First_Sale_Date,
    fs.SALE_AMOUNT,

    -- Days calculations
    DATEDIFF(DAY, fls.CREATED_DATE, fa.EVENT_DATE) AS Days_LeadSource_To_Appointment,
    DATEDIFF(DAY, fa.EVENT_DATE, fs.SALE_DATE) AS Days_Appointment_To_Sale,
    DATEDIFF(DAY, fls.CREATED_DATE, fs.SALE_DATE) AS Days_LeadSource_To_Sale,

    -- Journey stage classification
    CASE
        WHEN fs.SALE_DATE IS NOT NULL THEN 'Converted to Sale'
        WHEN fa.EVENT_DATE IS NOT NULL THEN 'Had Appointment - No Sale'
        ELSE 'Lead Only - No Appointment'
    END AS Journey_Stage,

    -- Flags for filtering
    CASE WHEN fa.EVENT_DATE IS NOT NULL THEN 1 ELSE 0 END AS Had_Appointment,
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
-- AND fls.CREATED_DATE >= DATEADD(MONTH, -12, GETDATE())

ORDER BY fls.CREATED_DATE DESC;

/*
USAGE:
- Use this query to see individual contact journeys
- Export to Excel for detailed analysis
- Filter by specific lead sources for deep dives
- Useful for validating aggregated metrics
*/
