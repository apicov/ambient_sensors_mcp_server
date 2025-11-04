#!/usr/bin/env python3
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastmcp import FastMCP
import sqlparse
import uuid
import pandas as pd
from python_executor import PandasExecutor

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

@mcp.tool(description="Clear cached query results. Provide query_id to clear a specific query, or omit to clear all cached queries.")
def clear_query_cache(query_id: str = None) -> str:
    if query_id:
        if query_id in query_cache:
            del query_cache[query_id]
            return f"Cleared query {query_id}"
        return f"Query {query_id} not found"
    else:
        query_cache.clear()
        return "Cleared all cached queries"

@mcp.tool(description="Execute a read-only SQL SELECT query against the ambient sensors database. Returns a query_id for caching, along with metadata (row count, columns, data types) and a preview of the first 5 rows. Use this query_id with execute_pandas to analyze the results. Check the schema://database resource for table structure before writing queries.")
def execute_sql_query(sql: str) -> dict:
    
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
        
        # Return metadata
        return {
            "query_id": query_id,
            "rows": len(df),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "preview": df.head(5).to_dict('records')
        }

    except Exception as e:
        return {"error": f"Query execution failed: {str(e)}"}

@mcp.tool(description="Get a complete list of all available sensors in the database with their metadata (sensor_id, name, location, type, etc.). Use this to discover which sensors are available before querying sensor data.")
def list_sensors() -> str:
    cur.execute("SELECT * FROM sensors")
    results = cur.fetchall()
    description = [d.name for d in cur.description]
    resp_dict = create_sensor_dict(results, description)
    return str(resp_dict)

'''@mcp.tool(description="Get the most recent readings from a specific sensor by sensor_id. Returns up to 'limit' readings (default 10) sorted by timestamp descending. Use list_sensors first to find available sensor_id values.")
def get_sensor_data(sensor_id: str, limit: int = 10) -> str:
    query = """
        SELECT * FROM sensor_readings
        WHERE sensor_id = %s
        ORDER BY timestamp DESC
        LIMIT %s
    """
    cur.execute(query, (sensor_id, limit))
    results = cur.fetchall()
    description = [d.name for d in cur.description]
    
    readings = []
    for row in results:
        reading = {description[i]: row[i] for i in range(len(description))}
        readings.append(reading)
    
    return str(readings)
    '''

@mcp.tool(description="Execute Python/pandas code against a cached DataFrame from execute_sql_query. The query_id identifies which cached query result to use. The DataFrame is available as 'df' in your code. Use this for data analysis, transformations, visualizations, or calculations on query results.")
def execute_pandas(query_id: str, code: str) -> str:
    return pandasEx.execute_code(query_id, query_cache, code)

@mcp.resource("schema://database")
def get_database_schema() -> str:
    """Provide database schema information for the sensor database"""
    
    schema_info = []
    
    # Get all tables
    cur_col.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cur.fetchall()
    
    # For each table, get column details
    for (table_name,) in tables:
        cur_col.execute("""
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
