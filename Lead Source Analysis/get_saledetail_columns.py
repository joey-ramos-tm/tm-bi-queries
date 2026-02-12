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

    # Get sample data to see column names from SaleDetail table
    print("\n=== Querying SaleDetail table ===")
    cursor.execute("SELECT TOP 5 * FROM [TaylorMorrisonDWH_Gold].[Sales].[SaleDetail]")

    # Get column names from cursor description
    columns = [column[0] for column in cursor.description]
    print("\n=== Column Names ===")
    for i, col in enumerate(columns, 1):
        print(f"{i}. {col}")

    print(f"\n=== Sample Data (First 5 rows) ===")
    rows = cursor.fetchall()
    if rows:
        print(f"\nTotal columns: {len(columns)}")
        print("\nFirst row (first 20 columns):")
        for col, val in zip(columns[:20], rows[0][:20]):
            val_str = str(val)[:50] if val is not None else "NULL"
            print(f"  {col}: {val_str}")

        # Look for approval date column
        print("\n\nSearching for approval-related columns:")
        for col in columns:
            if 'APPROVAL' in col.upper() or 'APPROV' in col.upper():
                print(f"  Found: {col}")

        # Look for contact/account ID columns
        print("\nSearching for contact/account ID columns:")
        for col in columns:
            if 'CONTACT' in col.upper() or 'ACCOUNT' in col.upper() or '_ID' in col.upper():
                print(f"  Found: {col}")

    cursor.close()
    conn.close()
    print("\n\nSuccess! Use the column names above to update the SQL queries.")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
