from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route, Mount
from mcp.server import Server
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

# Create MCP Server app
app = Server("ambient-sensors-server")

sse = SseServerTransport("/messages/")

async def handle_sse(request: Request):
    async with sse.connect_sse(
        request.scope, 
        request.receive, 
        request._send
    ) as streams:
        await app.run(
            streams[0], 
            streams[1], 
            app.create_initialization_options()
        )
    return Response()

starlette_app = Starlette(
    routes=[
        Route("/sse", endpoint=handle_sse, methods=["GET"]),
        Mount("/messages/", app=sse.handle_post_message),
    ]
)

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



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        starlette_app, 
        host="0.0.0.0", 
        port=8000,
        ssl_keyfile="/etc/letsencrypt/live/thestitchpatterns.store/privkey.pem",
        ssl_certfile="/etc/letsencrypt/live/thestitchpatterns.store/fullchain.pem"
    )
