"""
Test access to Workday Job Requisition table in TaylorMorrisonDataLake
For BUS-310 - Open Job Requisitions Dashboard
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sql_connection import connect_to_datalake
import pandas as pd

print("=" * 100)
print("TESTING WORKDAY JOB REQUISITION TABLE ACCESS")
print("=" * 100)

try:
    # Connect to DataLake
    conn = connect_to_datalake()
    cursor = conn.cursor()

    # Test 1: List all schemas
    print("\n[TEST 1] Listing all schemas in TaylorMorrisonDataLake...")
    print("-" * 100)

    cursor.execute("""
        SELECT DISTINCT SCHEMA_NAME(schema_id) AS SchemaName
        FROM sys.objects
        WHERE SCHEMA_NAME(schema_id) IS NOT NULL
        ORDER BY SchemaName;
    """)

    schemas = cursor.fetchall()
    print(f"Found {len(schemas)} schemas:")
    for schema in schemas:
        print(f"  - {schema[0]}")

    # Test 2: Check for WorkDay schema
    print("\n[TEST 2] Checking for WorkDay schema and objects...")
    print("-" * 100)

    cursor.execute("""
        SELECT
            SCHEMA_NAME(schema_id) AS SchemaName,
            name AS ObjectName,
            type_desc AS ObjectType
        FROM sys.objects
        WHERE SCHEMA_NAME(schema_id) = 'WorkDay'
        ORDER BY name;
    """)

    workday_objects = cursor.fetchall()
    if workday_objects:
        print(f"Found {len(workday_objects)} objects in WorkDay schema:")
        for schema, name, obj_type in workday_objects:
            print(f"  - {schema}.{name} ({obj_type})")
    else:
        print("No WorkDay schema found")

    # Test 3: Query Get_Job_Requisition table
    print("\n[TEST 3] Querying [WorkDay].[Get_Job_Requisition] table...")
    print("-" * 100)

    cursor.execute("""
        SELECT TOP 10 *
        FROM [WorkDay].[Get_Job_Requisition]
        ORDER BY 1 DESC;
    """)

    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    print(f"SUCCESS! Table exists with {len(columns)} columns")
    print(f"Retrieved {len(rows)} sample rows")

    print(f"\nColumns ({len(columns)}):")
    for i, col in enumerate(columns, 1):
        print(f"  {i}. {col}")

    # Test 4: Get row count
    print("\n[TEST 4] Getting total row count...")
    print("-" * 100)

    cursor.execute("""
        SELECT COUNT(*) AS Total_Requisitions
        FROM [WorkDay].[Get_Job_Requisition];
    """)

    total_count = cursor.fetchone()[0]
    print(f"Total Job Requisitions in table: {total_count:,}")

    # Test 5: Get column details
    print("\n[TEST 5] Getting column details...")
    print("-" * 100)

    cursor.execute("""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'WorkDay'
          AND TABLE_NAME = 'Get_Job_Requisition'
        ORDER BY ORDINAL_POSITION;
    """)

    col_details = cursor.fetchall()
    print(f"\nColumn Details ({len(col_details)} columns):")
    for col_name, data_type, max_length, nullable in col_details:
        length_str = f"({max_length})" if max_length else ""
        null_str = "NULL" if nullable == "YES" else "NOT NULL"
        print(f"  {col_name}: {data_type}{length_str} {null_str}")

    # Test 6: Export sample data
    print("\n[TEST 6] Exporting sample data to CSV...")
    print("-" * 100)

    cursor.execute("""
        SELECT TOP 100 *
        FROM [WorkDay].[Get_Job_Requisition]
        ORDER BY 1 DESC;
    """)

    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    df = pd.DataFrame.from_records(rows, columns=columns)

    output_file = "T:\\Corp IT\\Scottsdale\\Bus Sys Analyst\\Contractor Share\\JRamos\\Jira Tickets\\Job Requisition\\Workday_Job_Requisitions_Sample.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"Sample data exported to: {output_file}")
    print(f"Rows exported: {len(df):,}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 100)
    print("SUCCESS! WORKDAY JOB REQUISITION DATA IS ACCESSIBLE!")
    print("=" * 100)
    print("\nNEXT STEPS:")
    print("1. Review the sample CSV file")
    print("2. Identify which columns contain JSON data")
    print("3. Create SQL script to parse JSON columns")
    print("4. Update BUS-310 action plan - DATA-490 IS COMPLETE!")
    print("=" * 100)

except Exception as e:
    print(f"\nERROR: {str(e)}")
    import traceback
    traceback.print_exc()
