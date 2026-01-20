"""
SQL Server Data Warehouse Connection Script
Connects to SQL Server using credentials from .env file
"""

import os
import pyodbc
from dotenv import load_dotenv

def connect_to_sql_server(database_name):
    """
    Establishes a connection to SQL Server data warehouse using credentials from .env

    Args:
        database_name: Name of the database to connect to

    Returns:
        pyodbc.Connection: Database connection object
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get credentials from environment variables
    server = os.getenv('SQL_SERVER')
    driver = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

    # Validate that all required credentials are present
    if not all([server, database_name]):
        raise ValueError("Missing required database credentials in .env file")

    # Build connection string using Windows Authentication
    connection_string = (
        f'DRIVER={{{driver}}};'
        f'SERVER={server};'
        f'DATABASE={database_name};'
        f'Trusted_Connection=yes;'
        f'Connection Timeout=30;'
    )

    try:
        # Establish connection
        connection = pyodbc.connect(connection_string)
        print(f"Successfully connected to {database_name} on {server}")
        return connection

    except pyodbc.Error as e:
        print(f"Error connecting to SQL Server: {e}")
        raise


def connect_to_gold():
    """Connect to TaylorMorrisonDWH_Gold database"""
    load_dotenv()
    database = os.getenv('SQL_DATABASE_GOLD')
    return connect_to_sql_server(database)


def connect_to_silver():
    """Connect to TaylorMorrisonDWH_Silver database"""
    load_dotenv()
    database = os.getenv('SQL_DATABASE_SILVER')
    return connect_to_sql_server(database)

def main():
    """
    Main function to demonstrate database connection and query execution
    """
    try:
        # Connect to Gold database
        print("\n=== Connecting to Gold Database ===")
        conn_gold = connect_to_gold()
        cursor_gold = conn_gold.cursor()
        cursor_gold.execute("SELECT DB_NAME() AS current_database")
        row = cursor_gold.fetchone()
        print(f"Connected to: {row.current_database}")
        cursor_gold.close()
        conn_gold.close()

        print("\n=== Connecting to Silver Database ===")
        conn_silver = connect_to_silver()
        cursor_silver = conn_silver.cursor()
        cursor_silver.execute("SELECT DB_NAME() AS current_database")
        row = cursor_silver.fetchone()
        print(f"Connected to: {row.current_database}")
        cursor_silver.close()
        conn_silver.close()

        print("\nAll connections successful and closed")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
