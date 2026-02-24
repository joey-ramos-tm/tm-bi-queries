"""
Check if TaylorMorrisonDataLake.WorkDay.Get_Job_Requisition exists and get its schema
For BUS-310 - Open Job Requisitions Dashboard
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sql_connection import connect_to_gold
import pandas as pd

print("=" * 100)
print("CHECKING WORKDAY JOB REQUISITION TABLE SCHEMA")
print("=" * 100)

try:
    conn = connect_to_gold()
    cursor = conn.cursor()

    # Try Method 1: Check if table exists in INFORMATION_SCHEMA
    print("\n[METHOD 1] Checking INFORMATION_SCHEMA for table...")
    print("-" * 100)

    try:
        query1 = """
        SELECT
            TABLE_CATALOG,
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE '%Job%Requisition%'
           OR TABLE_NAME LIKE '%Get_Job%'
        ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME;
        """

        cursor.execute(query1)
        tables = cursor.fetchall()

        if tables:
            print(f"Found {len(tables)} matching tables:")
            for catalog, schema, name, table_type in tables:
                print(f"  - {catalog}.{schema}.{name} ({table_type})")
        else:
            print("No matching tables found in current database")

    except Exception as e:
        print(f"Error checking INFORMATION_SCHEMA: {str(e)}")

    # Try Method 2: Direct query attempt
    print("\n[METHOD 2] Attempting direct query...")
    print("-" * 100)

    try:
        query2 = """
        SELECT TOP 1 *
        FROM [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition];
        """

        cursor.execute(query2)
        columns = [column[0] for column in cursor.description]

        print(f"SUCCESS! Table exists with {len(columns)} columns:")
        for i, col in enumerate(columns, 1):
            print(f"  {i}. {col}")

        # Get column details
        cursor.execute("""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE
            FROM [TaylorMorrisonDataLake].INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'WorkDay'
              AND TABLE_NAME = 'Get_Job_Requisition'
            ORDER BY ORDINAL_POSITION;
        """)

        col_details = cursor.fetchall()

        if col_details:
            print("\nColumn Details:")
            for col_name, data_type, max_length, nullable in col_details:
                length_str = f"({max_length})" if max_length else ""
                print(f"  {col_name}: {data_type}{length_str} {'NULL' if nullable == 'YES' else 'NOT NULL'}")

    except Exception as e:
        print(f"ERROR: {str(e)}")
        print("\nTable [TaylorMorrisonDataLake].[WorkDay].[Get_Job_Requisition] does NOT exist or is not accessible")

    # Try Method 3: Check all databases for DataLake
    print("\n[METHOD 3] Searching all databases for DataLake...")
    print("-" * 100)

    try:
        query3 = """
        SELECT name
        FROM sys.databases
        WHERE name LIKE '%Lake%' OR name LIKE '%Workday%'
        ORDER BY name;
        """

        cursor.execute(query3)
        databases = cursor.fetchall()

        if databases:
            print(f"Found {len(databases)} databases with 'Lake' or 'Workday':")
            for db in databases:
                print(f"  - {db[0]}")
        else:
            print("No databases found with 'Lake' or 'Workday' in the name")

    except Exception as e:
        print(f"Error checking databases: {str(e)}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 100)
    print("TABLE SCHEMA CHECK COMPLETE")
    print("=" * 100)

except Exception as e:
    print(f"\nCRITICAL ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
