# Ambient Sensors MCP Server

An MCP (Model Context Protocol) server for collecting, storing, and analyzing ambient sensor data from ESP32 devices. The server provides tools for querying sensor data, performing statistical analysis, and generating visualizations through Claude Desktop or other MCP clients.

## Features

- **MQTT Data Collection**: Receives real-time sensor data from ESP32 devices via MQTT
- **Dual Database Storage**: Supports both columnar (sensor-specific tables) and flexible (metric-type based) database schemas
- **SQL Query Tool**: Execute safe, read-only SQL queries against the sensor database
- **Data Analysis**: Run pandas analysis code on query results in isolated Docker containers
- **Plot Generation**: Create matplotlib visualizations from query results
- **Device Monitoring**: Monitor device activity and send Pushover notifications for inactive devices
- **MCP Integration**: Expose all functionality through FastMCP tools for use with Claude Desktop

## Architecture

### Components

1. **[mcp_server_http.py](mcp_server_http.py)** - Main MCP server exposing tools via HTTP
   - SQL query execution with safety validation
   - Database schema inspection
   - Data analysis and plotting tools
   - Query result caching and file serving

2. **[sensor_collector.py](sensor_collector.py)** - MQTT data collector
   - Connects to MQTT broker and subscribes to device topics
   - Stores sensor data in PostgreSQL databases
   - Supports both columnar and flexible database schemas
   - Auto-reconnection and error handling

3. **[python_executor.py](python_executor.py)** - Sandboxed code execution
   - Executes user-provided pandas and matplotlib code in Docker containers
   - Isolated execution environment with network disabled
   - Memory limits and timeout protection

4. **[device_activity_inspector.py](device_activity_inspector.py)** - Device health monitoring
   - Monitors sensor device activity
   - Sends Pushover notifications when devices go offline
   - Configurable check intervals and inactivity thresholds

## Database Schema

### Columnar Database (`ambient_sensors_columnar`)

Sensor-specific tables optimized for time-series queries:

```
devices
├── device_id (PK)
├── device_name
├── location
└── firmware_version

sensors
├── sensor_id (PK)
├── device_id (FK)
├── sensor_type
├── location
└── metadata (JSONB)

scd30_measurements
├── time
├── sensor_id (FK)
├── co2
├── temperature
└── humidity

bmp280_measurements
├── time
├── sensor_id (FK)
├── pressure
├── temperature
└── humidity
```

### Flexible Database (`ambient_sensors_flexible`)

Generic schema supporting any sensor type:

```
measurements
├── time
├── sensor_id (FK)
├── metric_type
└── value
```

## Installation

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Docker (for sandboxed code execution)
- MQTT broker
- Pushover account (optional, for device monitoring)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd mcp_sensor_server
```

2. Install Python dependencies:
```bash
pip install fastmcp psycopg2-binary pandas sqlparse python-dotenv paho-mqtt docker requests
```

3. Create `.env` file with configuration:
```env
DB_HOST=your_database_host
DB_USER=your_database_user
DB_PASSWORD=your_database_password

MQTT_BROKER=your_mqtt_broker_host
MQTT_PORT=1883

PYTHON_PROJECT_FOLDER=/path/to/sandbox
DOCKER_IMAGE=continuumio/miniconda3

SERVER_URL=http://localhost:8000

# Optional: Device monitoring
PUSHOVER_USER=your_pushover_user_key
PUSHOVER_TOKEN=your_pushover_app_token
CHECK_INTERVAL=600
INACTIVITY_THRESHOLD=300
```

4. Set up PostgreSQL databases (columnar and/or flexible schema)

## Usage

### Running the MCP Server

Start the HTTP server:
```bash
python mcp_server_http.py
```

For HTTPS (requires SSL certificates):
```bash
python mcp_server_http.py https
```

The server will start on:
- HTTP: `http://0.0.0.0:8000`
- HTTPS: `https://0.0.0.0:8001`

### Running the MQTT Collector

Start collecting sensor data:
```bash
python sensor_collector.py
```

The collector will:
- Connect to the MQTT broker
- Subscribe to device topics
- Store incoming sensor data in configured databases
- Auto-reconnect on connection loss

### Running the Device Monitor

Start device health monitoring:
```bash
python device_activity_inspector.py
```

### Configuring Claude Desktop

Add to your Claude Desktop MCP settings:

```json
{
  "mcpServers": {
    "ambient-sensors": {
      "url": "http://localhost:8000/mcp/"
    }
  }
}
```

## Available MCP Tools

### `get_database_schema()`
Returns the complete database schema including all tables and columns.

### `list_sensors()`
Lists all available sensors with their metadata (sensor_id, name, location, type).

### `execute_sql_query(sql: str)`
Executes a read-only SELECT query against the database. Returns query_id and CSV download link.

**Example:**
```sql
SELECT * FROM scd30_measurements WHERE sensor_id = 1 ORDER BY time DESC LIMIT 100
```

### `analyze_data(query_id: str, code: str)`
Executes pandas analysis code on a query result. The DataFrame is available as `df`.

**Example:**
```python
print(df.describe())
print("\nCorrelation matrix:")
print(df.corr())
```

### `create_plot(query_id: str, plot_code: str)`
Creates a matplotlib plot from query results. The DataFrame is available as `df` and pyplot as `plt`.

**Example:**
```python
plt.plot(df['time'], df['temperature'])
plt.xlabel('Time')
plt.ylabel('Temperature (°C)')
plt.title('Temperature Over Time')
plt.xticks(rotation=45)
plt.tight_layout()
```

### `clear_query_cache(query_id: str = None)`
Clears cached query results. Provide query_id to clear specific query, or omit to clear all.

## Security

- **SQL Safety**: All SQL queries are validated to ensure they're read-only SELECT statements
- **Sandboxed Execution**: Python code runs in isolated Docker containers with:
  - Network disabled
  - Memory limits (128MB default)
  - Execution timeout (30s default)
  - All code execution is contained within Docker

## MQTT Topics

The system subscribes to the following MQTT topics:

- `devices/+/capabilities` - Device registration and capabilities
- `devices/+/sensors/+/data` - Sensor data readings
- `devices/+/status` - Device status updates
- `devices/+/error` - Device error messages

### Message Format

**Capabilities:**
```json
{
  "device_name": "ESP32-Living-Room",
  "firmware_version": "1.0.0",
  "device_location": "Living Room",
  "sensors": ["scd30", "bmp280"],
  "metadata": {
    "scd30": {"location": "wall-mounted"},
    "bmp280": {"location": "ceiling"}
  }
}
```

**Sensor Data:**
```json
{
  "timestamp": 1699564800,
  "value": {
    "temperature": {"reading": 22.5},
    "humidity": {"reading": 45.2},
    "co2": {"reading": 678}
  }
}
```

## File Structure

```
mcp_sensor_server/
├── mcp_server_http.py          # Main MCP server
├── sensor_collector.py          # MQTT data collector
├── python_executor.py           # Sandboxed code execution
├── device_activity_inspector.py # Device monitoring
├── sandbox/                     # Query results and plots
├── .env                         # Configuration
├── Dockerfile                   # Docker configuration
└── README.md
```

## Development

### Adding New Sensor Types

For columnar database, add sensor-specific storage method in `sensor_collector.py`:

```python
def _store_new_sensor(self, sensor_id, timestamp, values):
    """Store new sensor data in new_sensor_measurements table"""
    conn = self.db_pool.getconn()
    try:
        cur = conn.cursor()
        # Insert logic here
        conn.commit()
    finally:
        self.db_pool.putconn(conn)
```

The flexible database automatically handles new sensor types without code changes.
