from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp.types as types
import asyncio
import signal
import sys
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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

conn = psycopg2.connect(**DB_CONFIG_COLUMNAR)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

def create_sensor_dict(results, description):
    sensor_dict = {}
    for row in results:
        sensor_id = row[0]
        sensor_info = {description[i]: row[i] for i in range(1, len(description))}
        sensor_dict[sensor_id] = sensor_info
    return sensor_dict

def list_sensors() -> dict:
    cur.execute("SELECT * FROM sensors")
    results = cur.fetchall()
    description = [d.name for d in cur.description]
    resp_dict = create_sensor_dict(results, description)
    return resp_dict


app = Server("sensor-data-server")

@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_sensors",
            description="Get list of available sensors from the database",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    if name == "list_sensors":
        sensors = list_sensors()
        result_text = str(sensors)
        
        return [types.TextContent(
            type="text",
            text=result_text
        )]
    
    raise ValueError(f"Unknown tool: {name}")



async def main():
    # Database connections (your existing code)
    global conn, cur
    
    try:
        # Run the MCP server with stdio transport
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
    finally:
        # Cleanup connections
        cur.close()
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")