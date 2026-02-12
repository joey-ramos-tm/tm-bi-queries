/*
Lead Source Analysis
Determines first lead source, days to first appointment, and days to sale
Optimized for performance with CTEs and proper filtering
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
        AccountId AS CONTACT_ID,
        QuoteReferenceName AS SALE_ID,
        ApprovalDate AS SALE_DATE,
        NetSalesPriceAmount AS SALE_AMOUNT,
        ROW_NUMBER() OVER (
            PARTITION BY AccountId
            ORDER BY ApprovalDate ASC
        ) AS rn
    FROM [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail]
    WHERE AccountId IS NOT NULL
        AND ApprovalDate IS NOT NULL
),

-- CTE 4: Combine all data with calculated days
LeadSourceMetrics AS (
    SELECT
        fls.CONTACT_ID,
        fls.LEAD_SRC_ID,
        fls.LEAD_SRC_NM,
        fls.LEAD_SRC_TXT,
        fls.CREATE_TMS AS Lead_Source_Date,
        fa.ACTVTY_DT AS First_Appointment_Date,
        fs.SALE_DATE AS First_Sale_Date,

        -- Calculate days from lead source to appointment
        CASE
            WHEN fa.ACTVTY_DT IS NOT NULL
            THEN DATEDIFF(DAY, fls.CREATE_TMS, fa.ACTVTY_DT)
            ELSE NULL
        END AS Days_LeadSource_To_Appointment,

        -- Calculate days from appointment to sale
        CASE
            WHEN fa.ACTVTY_DT IS NOT NULL AND fs.SALE_DATE IS NOT NULL
            THEN DATEDIFF(DAY, fa.ACTVTY_DT, fs.SALE_DATE)
            ELSE NULL
        END AS Days_Appointment_To_Sale,

        -- Calculate total days from lead source to sale
        CASE
            WHEN fs.SALE_DATE IS NOT NULL
            THEN DATEDIFF(DAY, fls.CREATE_TMS, fs.SALE_DATE)
            ELSE NULL
        END AS Days_LeadSource_To_Sale,

        -- Flags for analysis
        CASE WHEN fa.ACTVTY_DT IS NOT NULL THEN 1 ELSE 0 END AS Had_Appointment,
        CASE WHEN fs.SALE_DATE IS NOT NULL THEN 1 ELSE 0 END AS Had_Sale,

        fs.SALE_AMOUNT

    FROM FirstLeadSource fls
    LEFT JOIN FirstAppointment fa
        ON fls.CONTACT_ID = fa.CONTACT_ID
        AND fa.rn = 1
    LEFT JOIN FirstSale fs
        ON fls.CONTACT_ID = fs.CONTACT_ID
        AND fs.rn = 1
    WHERE fls.rn = 1  -- Only first lead source per contact
)

-- CTE 5: Calculate aggregated metrics by Lead Source
AggregatedMetrics AS (
    SELECT
        LEAD_SRC_NM,
        LEAD_SRC_TXT,

        -- Contact counts
        COUNT(DISTINCT CONTACT_ID) AS Total_Contacts,
        SUM(Had_Appointment) AS Total_Appointments,
        SUM(Had_Sale) AS Total_Sales,

        -- Conversion rates
        CAST(SUM(Had_Appointment) * 100.0 / COUNT(DISTINCT CONTACT_ID) AS DECIMAL(5,2)) AS Appointment_Conversion_Rate,
        CAST(SUM(Had_Sale) * 100.0 / COUNT(DISTINCT CONTACT_ID) AS DECIMAL(5,2)) AS Sale_Conversion_Rate,
        CAST(SUM(Had_Sale) * 100.0 / NULLIF(SUM(Had_Appointment), 0) AS DECIMAL(5,2)) AS Appointment_To_Sale_Rate,

        -- Average days metrics
        AVG(Days_LeadSource_To_Appointment) AS Avg_Days_To_First_Appointment,
        AVG(Days_Appointment_To_Sale) AS Avg_Days_Appointment_To_Sale,
        AVG(Days_LeadSource_To_Sale) AS Avg_Days_To_Sale,

        -- Revenue metrics
        SUM(SALE_AMOUNT) AS Total_Revenue,
        AVG(SALE_AMOUNT) AS Avg_Sale_Amount

    FROM LeadSourceMetrics
    GROUP BY LEAD_SRC_NM, LEAD_SRC_TXT
    HAVING COUNT(DISTINCT CONTACT_ID) >= 10  -- Only include lead sources with at least 10 contacts
),

-- CTE 6: Calculate median metrics separately
MedianMetrics AS (
    SELECT
        LEAD_SRC_NM,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Days_LeadSource_To_Appointment) AS Median_Days_To_Appointment,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Days_Appointment_To_Sale) AS Median_Days_Appointment_To_Sale,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Days_LeadSource_To_Sale) AS Median_Days_To_Sale
    FROM LeadSourceMetrics
    WHERE LEAD_SRC_NM IN (SELECT LEAD_SRC_NM FROM AggregatedMetrics)
    GROUP BY LEAD_SRC_NM
)

-- Final results combining aggregated and median metrics
SELECT
    am.*,
    mm.Median_Days_To_Appointment,
    mm.Median_Days_Appointment_To_Sale,
    mm.Median_Days_To_Sale
FROM AggregatedMetrics am
LEFT JOIN MedianMetrics mm ON am.LEAD_SRC_NM = mm.LEAD_SRC_NM
ORDER BY am.Total_Sales DESC, am.Total_Appointments DESC;


/*
OPTIMIZATION NOTES:
1. Uses CTEs with ROW_NUMBER() to get first occurrences efficiently
2. Filters NULL values early to reduce dataset size
3. Uses LEFT JOINs appropriately to preserve lead source data
4. Aggregates only after all filtering is complete
5. HAVING clause filters after aggregation for efficiency

RECOMMENDED INDEXES:
CREATE INDEX idx_lead_src_contact_created ON [TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC] (CONTACT_ID, CREATE_TMS);
CREATE INDEX idx_event_contact_date ON [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT] (CONTACT_ID, ACTVTY_DT) WHERE APP_TYPE_HANDLE_CD IN ('appointment', 'In Person Tour', 'Virtual Tour', 'Virtual Appointment');
CREATE INDEX idx_sale_contact_date ON [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail] (AccountId, ApprovalDate) WHERE ApprovalDate IS NOT NULL;

USAGE NOTES:
- The APP_TYPE_HANDLE_CD filter includes common appointment types (appointment, In Person Tour, Virtual Tour, Virtual Appointment)
- Adjust the HAVING clause threshold (currently 10) based on your data volume
- Consider adding date range filters for recent data analysis
- Sales are identified by ApprovalDate IS NOT NULL in SaleDetail table
*/
