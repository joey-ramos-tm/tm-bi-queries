"""
Database Service Module
Provides database connection management for FastAPI
"""
import sys
import os
from typing import Generator
import pyodbc

# Add parent directory to path to import sql_connection
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


def connect_to_sandbox():
    """Connect to sandbox_BI database on SQLDWH1"""
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SQLDWH1.TWC.PVT;"
        "DATABASE=sandbox_BI;"
        "Trusted_Connection=yes;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


class DatabaseService:
    """Database service for managing SQL Server connections"""

    @staticmethod
    def get_connection() -> pyodbc.Connection:
        """
        Get a connection to the sandbox_BI database

        Returns:
            pyodbc.Connection: Active database connection
        """
        return connect_to_sandbox()

    @staticmethod
    def get_cursor(connection: pyodbc.Connection) -> pyodbc.Cursor:
        """
        Get a cursor from an active connection

        Args:
            connection: Active database connection

        Returns:
            pyodbc.Cursor: Database cursor
        """
        return connection.cursor()


def get_db() -> Generator[pyodbc.Connection, None, None]:
    """
    FastAPI dependency for database connections
    Yields a connection and ensures it's properly closed

    Usage:
        @app.get("/endpoint")
        def endpoint(db: pyodbc.Connection = Depends(get_db)):
            cursor = db.cursor()
            ...
    """
    connection = None
    try:
        connection = DatabaseService.get_connection()
        yield connection
    finally:
        if connection:
            connection.close()


def execute_query(query: str, params: tuple = None) -> list:
    """
    Execute a SQL query and return results as a list of dictionaries

    Args:
        query: SQL query string
        params: Optional tuple of query parameters

    Returns:
        list: List of dictionaries with column names as keys
    """
    connection = DatabaseService.get_connection()
    try:
        cursor = connection.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Get column names
        columns = [column[0] for column in cursor.description]

        # Fetch all rows and convert to dictionaries
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))

        return results

    finally:
        if connection:
            connection.close()
