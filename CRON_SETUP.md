# Cron Setup for File Cleanup

## Overview

The `cleanup_old_files.py` script removes files older than 7 days from the `PYTHON_PROJECT_FOLDER` directory to prevent disk space accumulation.

## Setup Instructions

### 1. Test the Script

First, test the script manually to ensure it works:

```bash
cd /home/pico/code/mcp_sensor_server
python cleanup_old_files.py
```

### 2. Configure Cron Job

Open your crontab for editing:

```bash
crontab -e
```

### 3. Add Daily Cron Job

Add one of the following lines to run the cleanup daily:

#### Option A: Run at 2:00 AM daily
```cron
0 2 * * * cd /home/pico/code/mcp_sensor_server && /usr/bin/python3 cleanup_old_files.py >> /tmp/cleanup_old_files.log 2>&1
```

#### Option B: Run at midnight daily
```cron
0 0 * * * cd /home/pico/code/mcp_sensor_server && /usr/bin/python3 cleanup_old_files.py >> /tmp/cleanup_old_files.log 2>&1
```

#### Option C: Run at 3:30 AM on Sundays only
```cron
30 3 * * 0 cd /home/pico/code/mcp_sensor_server && /usr/bin/python3 cleanup_old_files.py >> /tmp/cleanup_old_files.log 2>&1
```

### 4. Verify Cron Job

List your cron jobs to verify:

```bash
crontab -l
```

### 5. Check Logs

View the cleanup log:

```bash
tail -f /tmp/cleanup_old_files.log
```

## Cron Syntax Explained

```
* * * * * command
│ │ │ │ │
│ │ │ │ └─── Day of week (0-7, 0 and 7 are Sunday)
│ │ │ └───── Month (1-12)
│ │ └─────── Day of month (1-31)
│ └───────── Hour (0-23)
└─────────── Minute (0-59)
```

## Configuration

To change the maximum file age, edit `cleanup_old_files.py`:

```python
MAX_AGE_DAYS = 7  # Change to desired number of days
```

## Troubleshooting

### Cron job not running?

1. Check cron service is running:
   ```bash
   systemctl status cron
   ```

2. Check system logs:
   ```bash
   grep CRON /var/log/syslog
   ```

3. Verify Python path:
   ```bash
   which python3
   ```

### .env file not loaded?

Make sure the `.env` file exists in the same directory as the script and contains:
```env
PYTHON_PROJECT_FOLDER=/home/pico/code/mcp_sensor_server/sandbox
```

Alternatively, you can hardcode the path in the cron command:
```bash
0 2 * * * cd /home/pico/code/mcp_sensor_server && PYTHON_PROJECT_FOLDER=/home/pico/code/mcp_sensor_server/sandbox /usr/bin/python3 cleanup_old_files.py >> /tmp/cleanup_old_files.log 2>&1
```

## Manual Cleanup

To manually run cleanup with custom settings:

```bash
# Clean files older than 3 days
python cleanup_old_files.py  # Then edit MAX_AGE_DAYS in the script

# Or run directly
python3 -c "
import os
from pathlib import Path
import time

folder = Path('/home/pico/code/mcp_sensor_server/sandbox')
max_age = 3 * 24 * 60 * 60  # 3 days in seconds
current_time = time.time()

for file in folder.iterdir():
    if file.is_file() and (current_time - file.stat().st_mtime) > max_age:
        file.unlink()
        print(f'Deleted: {file.name}')
"
```
