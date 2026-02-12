import pyodbc
import sys

# Connection string for sandbox_BI
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER=SQLDWH1.TWC.PVT;"
    f"DATABASE=sandbox_BI;"
    f"Trusted_Connection=yes;"
)

try:
    print("Connecting to sandbox_BI database...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get sample data to see column names from EVENT table
    print("\n=== Querying EVENT table ===")
    cursor.execute("SELECT TOP 5 * FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT] WHERE APP_TYPE_HANDLE_CD = 'APP_TYPE_HANDLE_CD'")

    # Get column names from cursor description
    columns = [column[0] for column in cursor.description]
    print("\n=== Column Names ===")
    for i, col in enumerate(columns, 1):
        print(f"{i}. {col}")

    print(f"\n=== Sample Data (First 5 rows) ===")
    rows = cursor.fetchall()
    if rows:
        print(f"\nTotal columns: {len(columns)}")
        print("\nFirst row:")
        for col, val in zip(columns, rows[0]):
            val_str = str(val)[:50] if val is not None else "NULL"
            print(f"  {col}: {val_str}")
    else:
        print("No rows found with APP_TYPE_HANDLE_CD = 'APP_TYPE_HANDLE_CD'")
        print("\nLet me check what values exist for APP_TYPE_HANDLE_CD:")
        cursor.execute("SELECT DISTINCT TOP 10 APP_TYPE_HANDLE_CD FROM [TaylorMorrisonDWH_Silver].[SILVER_DB].[EVENT] WHERE APP_TYPE_HANDLE_CD IS NOT NULL")
        distinct_vals = cursor.fetchall()
        for val in distinct_vals:
            print(f"  {val[0]}")

    cursor.close()
    conn.close()
    print("\nSuccess! Use the column names above to update the SQL queries.")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
