#!/usr/bin/env python3
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from fastmcp import FastMCP
import sqlparse
import uuid

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


# Create FastMCP server
mcp = FastMCP("ambient-sensors-server")

def create_sensor_dict(results, description):
    """Helper function to create sensor dictionary from query results"""
    sensor_dict = {}
    for row in results:
        sensor_id = row[0]
        sensor_info = {description[i]: row[i] for i in range(1, len(description))}
        sensor_dict[sensor_id] = sensor_info
    return sensor_dict

@mcp.tool()
def list_sensors() -> str:
    """Get list of available sensors from the database."""
    cur.execute("SELECT * FROM sensors")
    results = cur.fetchall()
    description = [d.name for d in cur.description]
    resp_dict = create_sensor_dict(results, description)
    return str(resp_dict)

@mcp.tool()
def get_sensor_data(sensor_id: str, limit: int = 10) -> str:
    """Get recent data from a specific sensor."""
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
