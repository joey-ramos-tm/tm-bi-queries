import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SQLDWH1.TWC.PVT;"
    "DATABASE=sandbox_BI;"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Get row count
cursor.execute("SELECT COUNT(*) FROM [Demo].[JobRequisition]")
row_count = cursor.fetchone()[0]
print(f"Table [Demo].[JobRequisition] contains {row_count} rows")

# Get column count and names
cursor.execute("""
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='Demo' AND TABLE_NAME='JobRequisition'
""")
col_count = cursor.fetchone()[0]
print(f"Table has {col_count} columns")

# Show first few columns
cursor.execute("""
    SELECT TOP 10 COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='Demo' AND TABLE_NAME='JobRequisition'
    ORDER BY ORDINAL_POSITION
""")
print("\nFirst 10 columns:")
for row in cursor.fetchall():
    print(f"  - {row.COLUMN_NAME} ({row.DATA_TYPE})")

cursor.close()
conn.close()

print("\nTable created successfully!")
