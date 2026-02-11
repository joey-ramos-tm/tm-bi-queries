import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connection string for sandbox_BI
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER=SQLDWH1.TWC.PVT;"
    f"DATABASE=sandbox_BI;"
    f"Trusted_Connection=yes;"
    f"Connection Timeout=300;"
)

# Read the SQL query from file
with open(r"T:\Corp IT\Scottsdale\Bus Sys Analyst\Contractor Share\JRamos\Claude Code\Job Requisition\Candidate Stage sql.txt", 'r') as f:
    base_query = f.read()

# Add linked server prefix for Bronze database (on SQLDL1)
query = base_query.replace('[TaylorMorrisonDWH_Bronze]', '[SQLDL1].[TaylorMorrisonDWH_Bronze]')

# Add GETDATE() for timestamp
query = query.replace(
    'FROM [SQLDL1].[TaylorMorrisonDWH_Bronze].[WorkDay].[Candidate_Stage]',
    ''',GETDATE() AS TableRefreshedDate
FROM [SQLDL1].[TaylorMorrisonDWH_Bronze].[WorkDay].[Candidate_Stage]'''
)

# Wrap in CTE and add INTO clause with JOIN to JobRequisition
final_query = f"""
-- Drop table if exists
IF OBJECT_ID('[Demo].[JobReqCandidateStage]', 'U') IS NOT NULL
    DROP TABLE [Demo].[JobReqCandidateStage];

-- Create and populate table
WITH CandidateStageData AS (
{query}
)
SELECT
    cs.*,
    jr.Division,
    jr.Area,
    jr.Job_Family_Group_Name
INTO [Demo].[JobReqCandidateStage]
FROM CandidateStageData cs
LEFT JOIN [Demo].[JobRequisition] jr
    ON cs.JobReq_Number = jr.Requisition;
"""

try:
    print("Connecting to sandbox_BI database...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    print("Creating and populating JobReqCandidateStage table...")
    print("This may take several minutes due to cross-database joins and JSON parsing...")

    cursor.execute(final_query)
    conn.commit()

    print("Table [sandbox_BI].[Demo].[JobReqCandidateStage] created successfully!")

    # Get row count
    cursor.execute("SELECT COUNT(*) FROM [Demo].[JobReqCandidateStage]")
    row_count = cursor.fetchone()[0]
    print(f"Loaded {row_count} rows into the table")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
    raise
