import pyodbc

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

    # Try different table path variations
    table_variations = [
        "[SQLDL1].[TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]",
        "[TaylorMorrisonDWH_Silver].[SLS_MKT_VW].[LEAD_SRC]",
        "[SQLDL1].TaylorMorrisonDWH_Silver.SLS_MKT_VW.LEAD_SRC",
        "SQLDL1.TaylorMorrisonDWH_Silver.SLS_MKT_VW.LEAD_SRC",
    ]

    for table_path in table_variations:
        try:
            print(f"\nTrying: {table_path}")
            cursor.execute(f"SELECT TOP 1 * FROM {table_path}")
            columns = [column[0] for column in cursor.description]
            print(f"SUCCESS! Found table at: {table_path}")
            print(f"\nColumns ({len(columns)} total):")
            for i, col in enumerate(columns, 1):
                print(f"  {i}. {col}")

            # Get first row
            row = cursor.fetchone()
            if row:
                print("\nSample values from first row:")
                for col, val in zip(columns[:10], row[:10]):  # Show first 10 columns
                    val_str = str(val)[:50] if val is not None else "NULL"
                    print(f"  {col}: {val_str}")
            break
        except Exception as e:
            print(f"  Failed: {str(e)[:100]}")
            continue

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Connection Error: {e}")
