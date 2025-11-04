#!/usr/bin/env python3
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastmcp import FastMCP
import sqlparse
import uuid
import pandas as pd
from python_executor import PandasExecutor, MatplotlibExecutor
import json
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# Database configuration
DB_CONFIG_COLUMNAR = {
    'host': os.getenv("DB_HOST"),
    'database': 'ambient_sensors_columnar',
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': 5432
}

# Establish database connection
conn = psycopg2.connect(**DB_CONFIG_COLUMNAR)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Store query results
query_cache = {}

pandasEx = PandasExecutor()
matplotlibEx = MatplotlibExecutor()

csv_folder = os.getenv("PYTHON_PROJECT_FOLDER")

# Create FastMCP server
mcp = FastMCP("ambient-sensors-server")


def is_safe_query(sql: str) -> bool:
    """Validate that SQL query is read-only (SELECT only)"""
    # Forbidden keywords that modify data
    forbidden = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER',
                 'CREATE', 'TRUNCATE', 'REPLACE', 'MERGE']

    # Parse and normalize the SQL
    parsed = sqlparse.parse(sql)
    if not parsed:
        return False

    # Get the first statement
    statement = parsed[0]

    # Check if it's a SELECT statement
    if statement.get_type() != 'SELECT':
        return False

    # Check for forbidden keywords in the entire query
    sql_upper = sql.upper()
    for keyword in forbidden:
        if keyword in sql_upper:
            return False

    return True

def create_sensor_dict(results, description):
    """Helper function to create sensor dictionary from query results"""
    sensor_dict = {}
    for row in results:
        sensor_id = row[0]
        sensor_info = {description[i]: row[i] for i in range(1, len(description))}
        sensor_dict[sensor_id] = sensor_info
    return sensor_dict

#@mcp.tool()
def clear_query_cache(query_id: str = None) -> str:
    '''
    Clear cached query results. Provide query_id to clear a specific query, or omit to clear all cached queries.
    '''
    if query_id:
        if query_id in query_cache:
            del query_cache[query_id]
            return f"Cleared query {query_id}"
        return f"Query {query_id} not found"
    else:
        query_cache.clear()
        return "Cleared all cached queries"

@mcp.tool()
def execute_sql_query(sql: str) -> dict:
    '''
    Execute a read-only SQL SELECT query against the ambient sensors database.
    Returns a path to a CSV file containing the query results and a query_id of the cached DataFrame for further analysis with execute_matplotlib tool.
    '''
    # Validate query is safe
    if not is_safe_query(sql):
        return {
            "error": "Query contains forbidden operations. Only SELECT queries are allowed."
        }

    try:
        # Execute query with read-only transaction
        conn.set_session(readonly=True)
        df = pd.read_sql_query(sql, conn)

        # Generate unique ID
        query_id = str(uuid.uuid4())

        # Cache the DataFrame
        query_cache[query_id] = df

        # Save dataframe as csv file
        csv_path = f"{query_id}.csv"
        df.to_csv(Path(csv_folder)/csv_path, index=False)

        return {"csv_path": str(Path(csv_folder)/csv_path), "query_id": query_id}

    except Exception as e:
        return {"error": f"Query execution failed: {str(e)}"}

@mcp.tool()
def list_sensors() -> str:
    '''
    Get a complete list of all available sensors in the database with their metadata (sensor_id, name, location, type, etc.).
    Use this to discover which sensors are available before querying sensor data.
    '''
    cur.execute("SELECT * FROM sensors")
    results = cur.fetchall()
    description = [d.name for d in cur.description]
    resp_dict = create_sensor_dict(results, description)
    return str(resp_dict)

#@mcp.tool()
def execute_pandas(query_id: str, code: str) -> str:
    '''
    Execute Python/pandas code against a cached DataFrame from execute_sql_query. The query_id identifies which cached query result to use.
    The DataFrame is available as 'df' in your code. Use this for data analysis, transformations, visualizations, or calculations on query results.
    Use print() for retrieving required data.
    Use base64 encoding to print and retrieve images.
    '''
    return pandasEx.execute_code(query_id, query_cache, code)

@mcp.tool()
def execute_matplotlib(query_id: str, plot_code: str) -> str:
    '''
    Execute Python/matplotlib code against a cached DataFrame from execute_sql_query. The query_id identifies which cached query result to use.
    The DataFrame is available as 'df' in your code. Use this for creating visualizations based on query results.
    Use plt.show() to generate and save plots. The generated plot will be saved as an image file and a download link will be provided.
    '''
    return matplotlibEx.create_plot(query_id, query_cache, plot_code)


@mcp.tool()
def get_database_schema() -> str:
    '''
    Provide database schema information for the sensor database. Use it before starting sql queries.
    '''
    schema_info = []

    # Get all tables
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = cur.fetchall()

    # For each table, get column details
    for (table_name,) in tables:
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))

        columns = cur.fetchall()

        schema_info.append(f"\nTable: {table_name}")
        for col_name, data_type, nullable in columns:
            schema_info.append(f"  - {col_name}: {data_type} {'(nullable)' if nullable == 'YES' else ''}")

    return "\n".join(schema_info)

if __name__ == "__main__":
    # Run as local stdio server
    mcp.run()
