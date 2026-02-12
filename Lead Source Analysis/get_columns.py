import pyodbc
import sys

# Connection string for sandbox_BI (to use linked server to SQLDL1)
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

    # Get sample data to see column names
    print("\n=== Querying LEAD_SRC table ===")
    cursor.execute("SELECT TOP 5 * FROM [SQLDL1].[TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]")

    # Get column names from cursor description
    columns = [column[0] for column in cursor.description]
    print("\n=== Column Names ===")
    for i, col in enumerate(columns, 1):
        print(f"{i}. {col}")

    print("\n=== Sample Data (First 5 rows) ===")
    rows = cursor.fetchall()
    if rows:
        print(f"\nTotal columns: {len(columns)}")
        print("\nFirst row:")
        for col, val in zip(columns, rows[0]):
            print(f"  {col}: {val}")

    cursor.close()
    conn.close()
    print("\n✓ Success! Use the column names above to update the SQL queries.")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
