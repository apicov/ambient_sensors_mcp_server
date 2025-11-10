"""
Device Activity Inspector

Monitors sensor devices in the database and sends Pushover notifications
when devices haven't sent data within the configured inactivity threshold.
"""

import os
import time
import psycopg2
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

PUSHOVER_USER = os.getenv('PUSHOVER_USER')
PUSHOVER_TOKEN = os.getenv('PUSHOVER_TOKEN')
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 600))
INACTIVITY_THRESHOLD = int(os.getenv('INACTIVITY_THRESHOLD', 300))

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'port': 5432
}

def validate_env_variables():
    """
    Validate that all required environment variables are loaded.

    Raises:
        ValueError: If any required environment variable is missing
    """
    required_vars = {
        'PUSHOVER_USER': PUSHOVER_USER,
        'PUSHOVER_TOKEN': PUSHOVER_TOKEN,
        'DB_HOST': DB_CONFIG['host'],
        'DB_USER': DB_CONFIG['user'],
        'DB_PASSWORD': DB_CONFIG['password']
    }

    missing = [name for name, value in required_vars.items() if not value]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    print("✓ All required environment variables loaded")

def send_notification(message):
    """
    Send push notification via Pushover API.

    Args:
        message: Notification message text
    """
    payload = {
        "user": PUSHOVER_USER,
        "token": PUSHOVER_TOKEN,
        "message": message
    }
    try:
        requests.post("https://api.pushover.net/1/messages.json", data=payload, timeout=10)
        print(f"Notification sent: {message}")
    except Exception as e:
        print(f"Notification failed: {e}")

def get_devices(conn):
    """
    Retrieve all devices from the database.

    Args:
        conn: Database connection object

    Returns:
        List of tuples containing (device_id, device_name)
    """
    with conn.cursor() as cur:
        cur.execute("SELECT device_id, device_name FROM devices")
        return cur.fetchall()

def get_latest_message_time(conn, device_id):
    """
    Get the most recent message timestamp for a device across all its sensors.

    Args:
        conn: Database connection object
        device_id: Device identifier

    Returns:
        datetime object of the latest message, or None if no messages found
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(m.time) as last_seen
            FROM sensors s
            LEFT JOIN measurements m ON s.sensor_id = m.sensor_id
            WHERE s.device_id = %s
        """, (device_id,))
        result = cur.fetchone()
        return result[0] if result else None

def check_device_activity():
    """
    Check all devices for inactivity and send notifications for inactive devices.

    Queries the database for all devices and their latest message times.
    Sends Pushover notification if device hasn't sent data within INACTIVITY_THRESHOLD.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        devices = get_devices(conn)
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(seconds=INACTIVITY_THRESHOLD)

        for device_id, device_name in devices:
            latest = get_latest_message_time(conn, device_id)

            if latest is None:
                send_notification(f"⚠️ Device {device_name} ({device_id}) has never sent data")
            elif latest < threshold:
                minutes_ago = int((now - latest).total_seconds() / 60)
                send_notification(f"⚠️ Device {device_name} ({device_id}) inactive for {minutes_ago} minutes")
    finally:
        conn.close()

def main():
    """
    Main loop that periodically checks device activity.

    Runs continuously, checking devices every CHECK_INTERVAL seconds.
    Handles keyboard interrupt for graceful shutdown.
    """
    validate_env_variables()

    print(f"Device Activity Inspector started")
    print(f"Check interval: {CHECK_INTERVAL}s")
    print(f"Inactivity threshold: {INACTIVITY_THRESHOLD}s")

    while True:
        try:
            check_device_activity()
            time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            print("\nShutdown requested")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()

