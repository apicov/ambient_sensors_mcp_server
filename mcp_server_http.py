#!/usr/bin/env python3
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastmcp import FastMCP
import sqlparse
import uuid
import pandas as pd
from python_executor import AnalysisExecutor, MatplotlibExecutor
from starlette.staticfiles import StaticFiles
from starlette.routing import Mount
from pathlib import Path
import json

from dotenv import load_dotenv                                                                                                                                                                                                                                                                                                                                                    
load_dotenv() 

# Database configuration
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': 5432
}

# Establish database connection
conn = psycopg2.connect(**DB_CONFIG)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

# Directory for storing query results and plots
files_path = Path(os.getenv("PYTHON_PROJECT_FOLDER", "./query_results"))
files_path.mkdir(exist_ok=True)

# Server URL and port
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000")

# Initialize executors
analysisEx = AnalysisExecutor()
plotEx = MatplotlibExecutor()

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
    Clear cached query results. Provide query_id to clear a specific query CSV file, or omit to clear all cached query files.
    '''
    if query_id:
        csv_file = files_path / f"{query_id}.csv"
        if csv_file.exists():
            csv_file.unlink()
            return f"Cleared query {query_id}"
        return f"Query {query_id} not found"
    else:
        # Clear all CSV files
        count = 0
        for csv_file in files_path.glob("*.csv"):
            csv_file.unlink()
            count += 1
        return f"Cleared {count} cached query files"

@mcp.tool()
def execute_sql_query(sql: str) -> dict:
    '''
    Execute a read-only SQL SELECT query against the ambient sensors database.
    Returns query_id and CSV download link for all queries.
    Use query_id with analyze_data or create_plot tools for further analysis.
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

        # Save DataFrame to CSV
        csv_path = files_path / f"{query_id}.csv"
        df.to_csv(csv_path, index=False)

        # Return metadata
        return {
            "csv_download_link": f"{SERVER_URL}/files/{query_id}.csv",
            "query_id": query_id,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "csv_path": str(csv_path)
        }
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

@mcp.tool()
def analyze_data(query_id: str, code: str) -> str:
    '''
    Execute pandas analysis code on a query result DataFrame. The query_id identifies which query result to analyze.
    The DataFrame is available as 'df' in your code. Designed for statistical analysis with short outputs.
    Examples: df.describe(), df.corr(), df.groupby().mean(), df.value_counts()
    Use print() to display results.
    '''
    return analysisEx.analyze_data(query_id, str(files_path), code)

@mcp.tool()
def create_plot(query_id: str, plot_code: str) -> dict:
    '''
    Create a matplotlib plot from a query result DataFrame. The query_id identifies which query result to plot.
    The DataFrame is available as 'df', pyplot as 'plt' in your code.
    Write plotting code (e.g., plt.plot(df['x'], df['y']), plt.xlabel('X'), plt.title('My Plot')).
    Plot will be automatically saved with a UUID and download link will be returned.
    Do not add too many ticks on x axis, maximumm 12 unless  unless the user explicitly requests more.
    '''
    result = plotEx.create_plot(query_id, str(files_path), plot_code)

    return result

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

#@mcp.resource("guide://tools")
#    def get_tool_guide() -> str:
        
# Export app for uvicorn
app = mcp.http_app()

app.routes.append(
    Mount("/files", StaticFiles(directory=str(files_path)), name="files"))

if __name__ == "__main__":
    import sys
    
    # Check for command line argument
    if len(sys.argv) > 1 and sys.argv[1] == "https":
        print("Starting HTTPS server on port 8001...")
        import uvicorn
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8001,
            ssl_keyfile="/etc/letsencrypt/live/thestitchpatterns.store/privkey.pem",
            ssl_certfile="/etc/letsencrypt/live/thestitchpatterns.store/fullchain.pem"
        )
    else:
        print("Starting HTTP server on port 8000...")
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)
