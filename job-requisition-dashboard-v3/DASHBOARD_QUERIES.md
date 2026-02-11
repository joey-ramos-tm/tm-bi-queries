# Job Requisition Dashboard - SQL Queries Documentation

**Document Version**: 1.0
**Last Updated**: 2026-02-06
**Data Source**: `[TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]`

---

## Table of Contents

1. [Data Source Overview](#data-source-overview)
2. [Summary Statistics Query](#1-summary-statistics-query)
3. [Aging Analysis Query](#2-aging-analysis-query)
4. [Department Breakdown Query](#3-department-breakdown-query)
5. [Location Breakdown Query](#4-location-breakdown-query)
6. [Requisition Reasons Query](#5-requisition-reasons-query)
7. [Monthly Trends Query](#6-monthly-trends-query)
8. [Critical Requisitions Query](#7-critical-requisitions-query)
9. [Field Reference](#field-reference)
10. [Query Optimization Notes](#query-optimization-notes)

---

## Data Source Overview

**Primary Table**: `[TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]`

**Key Fields Used**:
- `Requisition` - Requisition ID
- `Job_Requisition_Status_Reference` - JSON field containing status (Open, Filled, etc.)
- `Recruiting_Start_Date` - Date requisition opened
- `Supervisory_Organization_Reference` - JSON field containing department name
- `Primary_Location_Reference` - JSON field containing location name
- `Job_Profile_Reference` - JSON field containing job title
- `RequisitionReason` - JSON field containing reason for requisition

**JSON Field Format**: Fields are stored as JSON arrays with structure: `[{"#text": "value"}]`

**Common Filter**: Most queries filter for open requisitions using:
```sql
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
```

---

## 1. Summary Statistics Query

**Purpose**: Provides high-level KPIs for the dashboard summary cards

**Endpoint**: `GET /api/analytics/summary`

**Location**: `backend/services/analytics_service.py:29`

**Dashboard Display**: Top summary cards showing Total Open, Departments, Locations, Avg Days Open

### SQL Query:

```sql
SELECT
    COUNT(*) AS Total_Open,
    COUNT(DISTINCT JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"')) AS Departments,
    COUNT(DISTINCT JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')) AS Locations,
    AVG(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open,
    MAX(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Max_Days_Open,
    COUNT(*) AS Total_Positions
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
```

### Returns:
- `Total_Open` - Total number of open requisitions
- `Departments` - Count of unique departments with open requisitions
- `Locations` - Count of unique locations with open requisitions
- `Avg_Days_Open` - Average number of days requisitions have been open
- `Max_Days_Open` - Longest time a requisition has been open
- `Total_Positions` - Total positions to be filled (same as Total_Open)

### Example Result:
```
Total_Open: 145
Departments: 23
Locations: 18
Avg_Days_Open: 42.3
Max_Days_Open: 178
Total_Positions: 145
```

---

## 2. Aging Analysis Query

**Purpose**: Groups open requisitions by age buckets for the aging chart

**Endpoint**: `GET /api/analytics/aging`

**Location**: `backend/services/analytics_service.py:70`

**Dashboard Display**: "Requisitions by Aging Bucket" bar chart

### SQL Query:

```sql
SELECT
    Age_Category,
    COUNT(*) AS Count
FROM (
    SELECT
        CASE
            WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 14
            THEN '0-14 Days'
            WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 30
            THEN '15-30 Days'
            WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 60
            THEN '31-60 Days'
            WHEN DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) <= 90
            THEN '61-90 Days'
            ELSE '90+ Days'
        END AS Age_Category
    FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
    WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
) AS AgeData
GROUP BY Age_Category
ORDER BY
    CASE Age_Category
        WHEN '0-14 Days' THEN 1
        WHEN '15-30 Days' THEN 2
        WHEN '31-60 Days' THEN 3
        WHEN '61-90 Days' THEN 4
        WHEN '90+ Days' THEN 5
    END
```

### Age Buckets:
- **0-14 Days** - Fresh requisitions (green indicator)
- **15-30 Days** - Recent requisitions (yellow indicator)
- **31-60 Days** - Moderate aging (orange indicator)
- **61-90 Days** - Aging requisitions (red indicator)
- **90+ Days** - Critical aging (dark red indicator)

### Returns:
- `Age_Category` - Name of age bucket
- `Count` - Number of requisitions in that bucket

### Example Result:
```
0-14 Days: 32
15-30 Days: 45
31-60 Days: 38
61-90 Days: 18
90+ Days: 12
```

---

## 3. Department Breakdown Query

**Purpose**: Shows top 10 departments with the most open requisitions

**Endpoint**: `GET /api/analytics/departments`

**Location**: `backend/services/analytics_service.py:118`

**Dashboard Display**: "Top 10 Departments" horizontal bar chart

### SQL Query:

```sql
SELECT TOP 10
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
    COUNT(*) AS Open_Count,
    AVG(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') IS NOT NULL
GROUP BY JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"')
ORDER BY Open_Count DESC
```

### Returns:
- `Department` - Department name
- `Open_Count` - Number of open requisitions in that department
- `Avg_Days_Open` - Average days open for requisitions in that department

### Example Result:
```
Sales: 28 (avg 35.2 days)
Construction: 22 (avg 48.1 days)
Land Development: 18 (avg 52.3 days)
Marketing: 12 (avg 28.5 days)
...
```

---

## 4. Location Breakdown Query

**Purpose**: Shows top 10 locations with the most open requisitions

**Endpoint**: `GET /api/analytics/locations`

**Location**: `backend/services/analytics_service.py:148`

**Dashboard Display**: "Top 10 Locations" horizontal bar chart

### SQL Query:

```sql
SELECT TOP 10
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
    COUNT(*) AS Open_Count,
    AVG(DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())) AS Avg_Days_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') IS NOT NULL
GROUP BY JSON_VALUE(Primary_Location_Reference, '$[1]."#text"')
ORDER BY Open_Count DESC
```

### Returns:
- `Location` - Location name
- `Open_Count` - Number of open requisitions at that location
- `Avg_Days_Open` - Average days open for requisitions at that location

### Example Result:
```
Phoenix, AZ: 35 (avg 42.1 days)
Austin, TX: 28 (avg 38.5 days)
Orlando, FL: 24 (avg 45.2 days)
Tampa, FL: 19 (avg 51.3 days)
...
```

---

## 5. Requisition Reasons Query

**Purpose**: Breaks down requisitions by reason (New Position, Replacement, etc.)

**Endpoint**: `GET /api/analytics/reasons`

**Location**: `backend/services/analytics_service.py:178`

**Dashboard Display**: "Top Reasons for Open Requisitions" donut chart

### SQL Query:

```sql
SELECT
    JSON_VALUE(RequisitionReason, '$[1]."#text"') AS Reason,
    COUNT(*) AS Count
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND JSON_VALUE(RequisitionReason, '$[1]."#text"') IS NOT NULL
GROUP BY JSON_VALUE(RequisitionReason, '$[1]."#text"')
ORDER BY Count DESC
```

### Returns:
- `Reason` - Reason for requisition (e.g., "New Position", "Replacement")
- `Count` - Number of requisitions with that reason

### Common Reasons:
- New Position
- Replacement
- Business Growth
- Backfill
- Promotion

### Example Result:
```
New Position: 78
Replacement: 45
Business Growth: 15
Backfill: 7
```

---

## 6. Monthly Trends Query

**Purpose**: Shows requisition trends over the last 12 months

**Endpoint**: `GET /api/analytics/trends`

**Location**: `backend/services/analytics_service.py:206`

**Dashboard Display**: "Trend Analysis Over Time" line chart with 3 lines

### SQL Query:

```sql
SELECT
    FORMAT(TRY_CAST(Recruiting_Start_Date AS DATE), 'yyyy-MM') AS Month,
    COUNT(*) AS Created,
    SUM(CASE WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Filled%'
             THEN 1 ELSE 0 END) AS Filled,
    SUM(CASE WHEN JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
             THEN 1 ELSE 0 END) AS Still_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE TRY_CAST(Recruiting_Start_Date AS DATE) >= DATEADD(MONTH, -12, GETDATE())
GROUP BY FORMAT(TRY_CAST(Recruiting_Start_Date AS DATE), 'yyyy-MM')
ORDER BY Month
```

### Returns:
- `Month` - Month in YYYY-MM format
- `Created` - Total requisitions created that month
- `Filled` - Requisitions that were filled
- `Still_Open` - Requisitions still open from that month

### Chart Lines:
1. **Blue line** - Total Created
2. **Green line** - Filled
3. **Red line** - Still Open

### Example Result:
```
2025-02: Created 42, Filled 28, Still_Open 14
2025-03: Created 38, Filled 32, Still_Open 6
2025-04: Created 45, Filled 35, Still_Open 10
...
```

---

## 7. Critical Requisitions Query

**Purpose**: Identifies requisitions open for 90+ days requiring immediate attention

**Endpoint**: `GET /api/analytics/critical`

**Location**: `backend/services/analytics_service.py:239`

**Dashboard Display**: "Critical Requisitions (90+ Days Open)" table at bottom of dashboard

### SQL Query:

```sql
SELECT TOP 20
    Requisition,
    JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
    JSON_VALUE(Supervisory_Organization_Reference, '$[1]."#text"') AS Department,
    JSON_VALUE(Primary_Location_Reference, '$[1]."#text"') AS Location,
    TRY_CAST(Recruiting_Start_Date AS DATE) AS Start_Date,
    DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) AS Days_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) > 90
ORDER BY Days_Open DESC
```

### Returns:
- `Requisition` - Requisition ID/Number
- `Job_Title` - Job position title
- `Department` - Department name
- `Location` - Location name
- `Start_Date` - Date requisition was opened
- `Days_Open` - Number of days open (all > 90)

### Purpose:
Shows the **top 20 most critical requisitions** that have been open the longest, allowing management to prioritize and take action.

### Example Result:
```
REQ-2023-1234 | Senior Construction Manager | Construction | Phoenix, AZ | 2023-08-15 | 178 days
REQ-2023-1456 | Sales Director | Sales | Austin, TX | 2023-09-01 | 162 days
REQ-2023-1789 | Project Manager | Land Development | Tampa, FL | 2023-09-15 | 148 days
...
```

---

## Field Reference

### JSON Field Extraction Pattern

The WorkDay data uses JSON arrays for many fields. The extraction pattern is:

```sql
JSON_VALUE(field_name, '$[1]."#text"')
```

**Breakdown**:
- `$` - Root of JSON
- `[1]` - Second element of array (index 1)
- `"#text"` - Property containing the text value

### Key Fields:

| Field Name | Description | Example Value |
|------------|-------------|---------------|
| `Requisition` | Unique requisition ID | REQ-2025-1234 |
| `Job_Requisition_Status_Reference` | Status (JSON) | Open, Filled, Cancelled |
| `Recruiting_Start_Date` | Date opened | 2025-01-15 |
| `Supervisory_Organization_Reference` | Department (JSON) | Sales, Construction |
| `Primary_Location_Reference` | Location (JSON) | Phoenix, AZ |
| `Job_Profile_Reference` | Job title (JSON) | Construction Manager |
| `RequisitionReason` | Reason (JSON) | New Position, Replacement |

### Date Calculations:

All date calculations use:
```sql
DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE())
```

- `TRY_CAST` - Safely converts string to DATE
- `DATEDIFF(DAY, ...)` - Calculates days between dates
- `GETDATE()` - Current date/time

---

## Query Optimization Notes

### Performance Considerations:

1. **JSON Field Extraction**:
   - JSON_VALUE operations are somewhat expensive
   - Consider adding computed columns for frequently accessed fields
   - Index computed columns for better performance

2. **Filtering**:
   - All queries filter on status first to reduce dataset
   - Status filter uses LIKE '%Open%' to catch variations

3. **Aggregations**:
   - COUNT DISTINCT operations on JSON fields can be slow
   - Consider materializing views for summary statistics

4. **Date Calculations**:
   - DATEDIFF calculations performed at query time
   - Consider adding a computed column for "Days_Open"

### Suggested Indexes:

```sql
-- Index on recruiting start date
CREATE INDEX IX_Recruiting_Start_Date
ON [WorkDay].[Get_Job_Requisition](Recruiting_Start_Date);

-- Computed column and index for status (if possible)
ALTER TABLE [WorkDay].[Get_Job_Requisition]
ADD Status_Text AS JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"');

CREATE INDEX IX_Status_Text
ON [WorkDay].[Get_Job_Requisition](Status_Text);
```

### Caching Strategy:

The dashboard uses React Query caching on the frontend:
- **Summary data**: Cached for 5 minutes
- **Charts**: Cached for 5 minutes
- **Critical requisitions**: Cached for 2 minutes
- Manual refresh available via "Refresh" button

---

## Testing Queries

You can test these queries directly in SQL Server Management Studio:

### Quick Test - Summary:
```sql
SELECT
    COUNT(*) AS Total_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
```

### Quick Test - Critical:
```sql
SELECT TOP 5
    Requisition,
    JSON_VALUE(Job_Profile_Reference, '$[1]."#text"') AS Job_Title,
    DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) AS Days_Open
FROM [TaylorMorrisonDWH_Bronze].[WorkDay].[Get_Job_Requisition]
WHERE JSON_VALUE(Job_Requisition_Status_Reference, '$[1]."#text"') LIKE '%Open%'
    AND DATEDIFF(DAY, TRY_CAST(Recruiting_Start_Date AS DATE), GETDATE()) > 90
ORDER BY Days_Open DESC
```

---

## API Endpoint Summary

| Endpoint | Query | Dashboard Display |
|----------|-------|-------------------|
| `/api/analytics/summary` | Query #1 | Summary cards at top |
| `/api/analytics/aging` | Query #2 | Aging bar chart |
| `/api/analytics/departments` | Query #3 | Departments horizontal bar |
| `/api/analytics/locations` | Query #4 | Locations horizontal bar |
| `/api/analytics/reasons` | Query #5 | Reasons donut chart |
| `/api/analytics/trends` | Query #6 | Monthly trends line chart |
| `/api/analytics/critical` | Query #7 | Critical requisitions table |

---

## Additional Resources

- **API Documentation**: http://localhost:8080/api/docs (when backend running)
- **Source Code**: `job-requisition-dashboard-v3/backend/services/analytics_service.py`
- **Data Models**: `job-requisition-dashboard-v3/backend/models/analytics.py`
- **Project README**: `job-requisition-dashboard-v3/README.md`

---

## Contact & Support

For questions about these queries or the dashboard:
- Check the API documentation at http://localhost:8080/api/docs
- Review the implementation at `backend/services/analytics_service.py`
- Consult the project README and implementation docs

---

**Document Status**: Complete
**Next Review**: As needed when queries are modified
