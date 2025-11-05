import asyncio
import httpx
import json
from mcp import ClientSession
from mcp.client.sse import sse_client

async def run_client():
    url = "https://thestitchpatterns.store:8000/sse"
    
    async with sse_client(url) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the connection
            await session.initialize()
            
            # List available tools
            tools = await session.list_tools()
            print("Available tools:", tools)
            
            # Call the list_sensors tool
            result = await session.call_tool("list_sensors", arguments={})
            print("\nSensor list result:")
            print(result)

if __name__ == "__main__":
    asyncio.run(run_client())