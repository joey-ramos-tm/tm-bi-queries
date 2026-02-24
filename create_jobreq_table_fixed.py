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
with open(r"T:\Corp IT\Scottsdale\Bus Sys Analyst\Contractor Share\JRamos\Claude Code\Job Requisition\Job Req SQL joined to PRSNEL.txt", 'r') as f:
    base_query = f.read()

# Add linked server prefix for Bronze database (on SQLDL1)
query = base_query.replace('[TaylorMorrisonDWH_Bronze]', '[SQLDL1].[TaylorMorrisonDWH_Bronze]')

# Fix duplicate column issue by replacing jp.* with explicit column list excluding Job_Profile_ID
query = query.replace(
    '\tjr.*\n\t,jp.*',
    '''\tjr.*
\t,jp.Job_Family_Name
\t,jp.Job_Family_Inactive
\t,jp.Job_Family_Group_ID
\t,jp.Job_Family_Group_Name
\t,jp.Job_Family_Group_Inactive
\t,jp.wd_type
\t,jp.wd_text
\t,jp.Job_Profile_Name'''
)

# Modify the final SELECT to include INTO clause
# Add INTO before the FROM jobReqs clause (after the column definitions)
query = query.replace(
    '\nFROM jobReqs jr',
    '\nINTO [Demo].[JobRequisition]\nFROM jobReqs jr'
)

# Add DROP TABLE at the beginning
query = f"""
-- Drop table if exists
IF OBJECT_ID('[Demo].[JobRequisition]', 'U') IS NOT NULL
    DROP TABLE [Demo].[JobRequisition];

{query}
"""

try:
    print("Connecting to sandbox_BI database...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    print("Executing query to create JobRequisition table...")
    print("This may take several minutes due to cross-database joins and large dataset...")

    cursor.execute(query)
    conn.commit()

    print("✓ Table [sandbox_BI].[Demo].[JobRequisition] created successfully!")

    # Get row count
    cursor.execute("SELECT COUNT(*) FROM [Demo].[JobRequisition]")
    row_count = cursor.fetchone()[0]
    print(f"✓ Loaded {row_count} rows into the table")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
    raise
