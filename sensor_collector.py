import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import psycopg2
from psycopg2 import pool
import json
import logging
import datetime
import time
import os
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_USERNAME = None  # Set if authentication required
MQTT_PASSWORD = None

# Database configuration
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': 5432
}


# ==================== DATABASE STORAGE CLASSES ====================

class DatabaseStorage:
    """Base class for database storage handlers"""
    def __init__(self, db_config, db_type):
        self.db_type = db_type
        self.sensor_id_cache = {}

        try:
            self.db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, **db_config)
            logger.info(f"✓ {db_type} database connection pool created")
        except Exception as e:
            logger.error(f"✗ Failed to create {db_type} database pool: {e}")
            raise

    def ensure_device_exists(self, device_id, payload):
        """Ensure device exists in database, create or update if not"""
        conn = self.db_pool.getconn()
        try:
            cur = conn.cursor()

            device_name = payload.get('device_name')
            firmware_version = payload.get('firmware_version')
            device_location = payload.get('device_location')

            cur.execute("""
                INSERT INTO devices (device_id, device_name, location, firmware_version)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (device_id)
                DO UPDATE SET
                    device_name = EXCLUDED.device_name,
                    location = EXCLUDED.location,
                    firmware_version = EXCLUDED.firmware_version
            """, (device_id, device_name, device_location, firmware_version))

            conn.commit()
            logger.info(f"✓ {self.db_type}: Device registered: {device_id} ({device_name}, firmware: {firmware_version}, location: {device_location})")
            cur.close()

        except Exception as e:
            conn.rollback()
            logger.error(f"{self.db_type}: Error ensuring device exists: {e}")
        finally:
            self.db_pool.putconn(conn)

    def ensure_sensor_exists(self, device_id, sensor_type, metadata):
        """Ensure sensor exists in database, create if not"""
        conn = self.db_pool.getconn()
        try:
            cur = conn.cursor()

            cur.execute("""
                SELECT sensor_id FROM sensors
                WHERE device_id = %s AND sensor_type = %s
            """, (device_id, sensor_type))

            result = cur.fetchone()

            if not result:
                sensor_meta = metadata.get(sensor_type, {})
                location = sensor_meta.get('location', 'unknown')

                # Store entire sensor metadata as JSONB
                cur.execute("""
                    INSERT INTO sensors (device_id, sensor_type, location, metadata)
                    VALUES (%s, %s, %s, %s)
                    RETURNING sensor_id
                """, (device_id, sensor_type, location, json.dumps(sensor_meta) if sensor_meta else None))

                sensor_id = cur.fetchone()[0]
                conn.commit()
                logger.info(f"✓ {self.db_type}: Created new sensor: {device_id}/{sensor_type} (ID: {sensor_id})")

                self.sensor_id_cache[f"{device_id}_{sensor_type}"] = sensor_id
            else:
                self.sensor_id_cache[f"{device_id}_{sensor_type}"] = result[0]

            cur.close()
        except Exception as e:
            conn.rollback()
            logger.error(f"{self.db_type}: Error ensuring sensor exists: {e}")
        finally:
            self.db_pool.putconn(conn)

    def get_sensor_id(self, device_id, sensor_type):
        """Get sensor_id from cache or database"""
        cache_key = f"{device_id}_{sensor_type}"

        if cache_key in self.sensor_id_cache:
            return self.sensor_id_cache[cache_key]

        conn = self.db_pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT sensor_id FROM sensors
                WHERE device_id = %s AND sensor_type = %s
            """, (device_id, sensor_type))

            result = cur.fetchone()
            cur.close()

            if result:
                self.sensor_id_cache[cache_key] = result[0]
                return result[0]

        except Exception as e:
            logger.error(f"{self.db_type}: Error getting sensor_id: {e}")
        finally:
            self.db_pool.putconn(conn)

        return None

    def close(self):
        """Close database connection pool"""
        if self.db_pool:
            self.db_pool.closeall()

    def store_sensor_data(self, device_id, sensor_type, timestamp, values):
        """Store sensor data - must be implemented by subclass"""
        raise NotImplementedError("Subclass must implement store_sensor_data()")


class FlexibleDatabaseStorage(DatabaseStorage):
    """Flexible database storage with metric_type-based measurements table"""
    def __init__(self, db_config):
        super().__init__(db_config, "Flexible")

    def store_sensor_data(self, device_id, sensor_type, timestamp, values):
        """Store sensor data in flexible measurements table"""
        sensor_id = self.get_sensor_id(device_id, sensor_type)

        if not sensor_id:
            logger.warning(f"{self.db_type}: Sensor not found: {device_id}/{sensor_type} - creating automatically")
            self.ensure_sensor_exists(device_id, sensor_type, {})
            sensor_id = self.get_sensor_id(device_id, sensor_type)

            if not sensor_id:
                logger.error(f"{self.db_type}: Failed to create sensor: {device_id}/{sensor_type}")
                return

        conn = self.db_pool.getconn()
        try:
            cur = conn.cursor()

            # Iterate through all fields and insert as separate rows with metric_type
            for field_name, field_data in values.items():
                reading = field_data.get('reading')
                if reading is not None:
                    cur.execute("""
                        INSERT INTO measurements (time, sensor_id, metric_type, value)
                        VALUES (%s, %s, %s, %s)
                    """, (timestamp, sensor_id, field_name, reading))

            conn.commit()
            logger.info(f"✓ {self.db_type}: TIME: {timestamp}, Sensor: {sensor_type}, Metrics: {list(values.keys())}")
            cur.close()

        except Exception as e:
            conn.rollback()
            logger.error(f"✗ {self.db_type}: Error storing sensor data: {e}")
        finally:
            self.db_pool.putconn(conn)


# ==================== MQTT COLLECTOR CLASS ====================

class SensorDataCollector:
    def __init__(self, mqtt_broker, mqtt_port, storage_handlers=None):
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.devices = {}  # Store device capabilities
        self.storage_handlers = storage_handlers if storage_handlers else []

        # Reconnection settings
        self.reconnect_delay = 5  # Start with 5 seconds
        self.max_reconnect_delay = 60  # Max 60 seconds
        self.connected = False
        
        # Setup MQTT client with VERSION2
        self.mqtt_client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect
        
        if MQTT_USERNAME and MQTT_PASSWORD:
            self.mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        """Subscribe to all device topics on connection"""
        if reason_code == 0:
            self.connected = True
            self.reconnect_delay = 5  # Reset delay on successful connection
            logger.info("✓ Connected to MQTT broker successfully")
            
            # Subscribe to all relevant topics
            client.subscribe("devices/+/capabilities", qos=1)
            client.subscribe("devices/+/sensors/+/data", qos=0)
            client.subscribe("devices/+/status", qos=1)
            client.subscribe("devices/+/error", qos=1)
            logger.info("✓ Subscribed to device topics")
        else:
            self.connected = False
            error_messages = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorized"
            }
            logger.error(f"✗ Failed to connect: {error_messages.get(reason_code, f'Unknown error {reason_code}')}")
    
    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        """Handle disconnection with auto-reconnect"""
        self.connected = False
        
        if reason_code == 0:
            logger.info("Clean disconnect from MQTT broker")
        else:
            logger.warning(f"⚠ Unexpected disconnect from MQTT broker (code: {reason_code})")
            logger.info(f"Will attempt reconnection in {self.reconnect_delay} seconds...")
    
    def on_message(self, client, userdata, msg):
        """Route messages based on topic"""
        try:
            topic_parts = msg.topic.split('/')
            
            if len(topic_parts) < 3:
                return
            
            device_id = topic_parts[1]
            message_type = topic_parts[2]
            
            payload = json.loads(msg.payload.decode())
            
            if message_type == "capabilities":
                self.handle_capabilities(device_id, payload)
            elif message_type == "sensors":
                # topic: devices/{device_id}/sensors/{sensor_id}/data
                sensor_id = topic_parts[3]
                self.handle_sensor_data(device_id, sensor_id, payload)
            elif message_type == "status":
                self.handle_status(device_id, payload)
            elif message_type == "error":
                self.handle_error(device_id, payload)
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON from {msg.topic}: {e}")
        except Exception as e:
            logger.error(f"Error processing message from {msg.topic}: {e}")
    
    def handle_capabilities(self, device_id, payload):
        """Store device capabilities and register in database"""
        self.devices[device_id] = payload
        logger.info(f"✓ Device {device_id} registered with {len(payload.get('sensors', []))} sensors")

        # Register device in all storage handlers
        for handler in self.storage_handlers:
            handler.ensure_device_exists(device_id, payload)

        # Register sensors in all storage handlers
        for sensor_type in payload.get('sensors', []):
            for handler in self.storage_handlers:
                handler.ensure_sensor_exists(device_id, sensor_type, payload.get('metadata', {}))
    
    def handle_sensor_data(self, device_id, sensor_id, payload):
        """Process and store sensor readings"""
        try:
            # Convert ESP32 timestamp to datetime
            timestamp = datetime.datetime.fromtimestamp(payload.get('timestamp', time.time()), tz=datetime.timezone.utc)

            # Extract sensor values
            values = payload.get('value', {})

            # Store to all storage handlers
            for handler in self.storage_handlers:
                handler.store_sensor_data(device_id, sensor_id, timestamp, values)

        except Exception as e:
            logger.error(f"Error storing sensor data for {device_id}/{sensor_id}: {e}")
    
    def handle_status(self, device_id, payload):
        """Handle device status changes"""
        status = payload.get('value', 'unknown')
        timestamp = datetime.datetime.fromtimestamp(payload.get('timestamp', time.time()), tz=datetime.timezone.utc)

        logger.info(f"Device {device_id} status: {status} at {timestamp}")
    
    def handle_error(self, device_id, payload):
        """Handle device errors"""
        error = payload.get('value', {})
        error_type = error.get('error_type', 'unknown')
        message = error.get('message', 'No message')
        severity = error.get('severity', 0)
        timestamp = datetime.datetime.fromtimestamp(payload.get('timestamp', time.time()), tz=datetime.timezone.utc)

        
        severity_labels = ['INFO', 'WARNING', 'ERROR', 'CRITICAL']
        severity_label = severity_labels[min(severity, 3)]
        
        logger.error(f"Device {device_id} [{severity_label}]: {error_type} - {message}")

    def connect_with_retry(self):
        """Connect to MQTT broker with exponential backoff"""
        while True:
            try:
                logger.info(f"Attempting to connect to {self.mqtt_broker}:{self.mqtt_port}...")
                self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
                return True
                
            except Exception as e:
                logger.error(f"✗ Connection failed: {e}")
                logger.info(f"Retrying in {self.reconnect_delay} seconds...")
                time.sleep(self.reconnect_delay)
                
                # Exponential backoff
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
    
    def start(self):
        """Connect to MQTT broker and start processing messages"""
        try:
            # Initial connection with retry
            self.connect_with_retry()
            
            logger.info("Starting message loop...")
            logger.info("Press Ctrl+C to stop")
            logger.info("-" * 50)
            
            # loop_forever() will auto-reconnect on disconnects
            self.mqtt_client.loop_forever(retry_first_connection=True)
            
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 50)
            logger.info("⚠ Shutdown requested...")
            self.stop()
        except Exception as e:
            logger.error(f"✗ Error in main loop: {e}")
            self.stop()
    
    def stop(self):
        """Clean shutdown"""
        logger.info("Stopping MQTT client...")
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

        logger.info("Closing database connections...")
        for handler in self.storage_handlers:
            handler.close()

        logger.info("✓ Shutdown complete")

def main():
    logger.info("=" * 50)
    logger.info("ESP32 Sensor Data Collector")
    logger.info("=" * 50)
    logger.info("Using Flexible Database")
    logger.info("=" * 50)

    # Create storage handler
    storage_handlers = [FlexibleDatabaseStorage(DB_CONFIG)]

    try:
        collector = SensorDataCollector(
            MQTT_BROKER,
            MQTT_PORT,
            storage_handlers=storage_handlers
        )
        collector.start()
    except Exception as e:
        logger.error(f"✗ Failed to start collector: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())