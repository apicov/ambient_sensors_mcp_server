#!/usr/bin/env python3
"""
Cleanup script for removing old query results and plots.

Removes files older than 7 days from the PYTHON_PROJECT_FOLDER directory.
Designed to run as a daily cron job to prevent disk space accumulation.
"""

import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configuration
PYTHON_PROJECT_FOLDER = os.getenv("PYTHON_PROJECT_FOLDER", "./sandbox")
MAX_AGE_DAYS = 7
MAX_AGE_SECONDS = MAX_AGE_DAYS * 24 * 60 * 60


def cleanup_old_files(folder_path: str, max_age_seconds: int) -> dict:
    """
    Remove files older than specified age from folder.

    Args:
        folder_path: Path to directory to clean
        max_age_seconds: Maximum age of files in seconds

    Returns:
        Dictionary with cleanup statistics
    """
    folder = Path(folder_path)

    if not folder.exists():
        return {
            "error": f"Folder does not exist: {folder_path}",
            "deleted": 0,
            "failed": 0,
            "total_size_freed": 0
        }

    if not folder.is_dir():
        return {
            "error": f"Path is not a directory: {folder_path}",
            "deleted": 0,
            "failed": 0,
            "total_size_freed": 0
        }

    current_time = time.time()
    deleted_count = 0
    failed_count = 0
    total_size_freed = 0
    deleted_files = []
    failed_files = []

    # Iterate through all files in directory
    for file_path in folder.iterdir():
        if not file_path.is_file():
            continue

        try:
            # Get file stats
            file_stat = file_path.stat()
            file_age = current_time - file_stat.st_mtime

            # Check if file is older than threshold
            if file_age > max_age_seconds:
                file_size = file_stat.st_size
                file_path.unlink()
                deleted_count += 1
                total_size_freed += file_size
                deleted_files.append(str(file_path.name))

        except Exception as e:
            failed_count += 1
            failed_files.append(f"{file_path.name}: {str(e)}")

    return {
        "deleted": deleted_count,
        "failed": failed_count,
        "total_size_freed": total_size_freed,
        "deleted_files": deleted_files,
        "failed_files": failed_files
    }


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def main():
    """Main cleanup function"""
    print("=" * 60)
    print("File Cleanup Script")
    print("=" * 60)
    print(f"Folder: {PYTHON_PROJECT_FOLDER}")
    print(f"Max age: {MAX_AGE_DAYS} days")
    print("-" * 60)

    result = cleanup_old_files(PYTHON_PROJECT_FOLDER, MAX_AGE_SECONDS)

    if "error" in result:
        print(f"ERROR: {result['error']}")
        return 1

    print(f"Files deleted: {result['deleted']}")
    print(f"Files failed: {result['failed']}")
    print(f"Space freed: {format_size(result['total_size_freed'])}")

    if result['deleted'] > 0:
        print("\nDeleted files:")
        for filename in result['deleted_files']:
            print(f"  - {filename}")

    if result['failed'] > 0:
        print("\nFailed files:")
        for failure in result['failed_files']:
            print(f"  - {failure}")

    print("=" * 60)
    print("Cleanup complete")

    return 0


if __name__ == "__main__":
    exit(main())
