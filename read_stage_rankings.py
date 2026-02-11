import pandas as pd

# Read the Excel file
excel_path = r"T:\Corp IT\Scottsdale\Bus Sys Analyst\Contractor Share\JRamos\Claude Code\Job Requisition\Candidate Stage ranking.xlsx"

try:
    df = pd.read_excel(excel_path)

    print("Excel file contents:")
    print("=" * 80)
    print(df.to_string())
    print("\n" + "=" * 80)
    print("\nColumn names:")
    print(df.columns.tolist())
    print("\nData types:")
    print(df.dtypes)
    print("\nShape:", df.shape)

except Exception as e:
    print(f"Error reading Excel file: {e}")
