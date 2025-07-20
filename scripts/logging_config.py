#!/usr/bin/env python3
"""
version 3.1.0
Centralized Logging Configuration

Key Features:
- Daily log rotation with automatic cleanup (30-day retention)
- Shared log files across all scripts for unified monitoring
- Console and file output with structured formatting
- Exception logging decorator for error tracking
- Cross-platform path handling and log management

Creates shared log files named 'analystbot_YYYY-MM-DD.log' for all bot operations.
"""

import os
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime, timedelta

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure logs directory exists
LOGS_DIR.mkdir(exist_ok=True)

# Logging configuration
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Logging settings
LOG_LEVEL = logging.INFO


def setup_logger(script_name: str, log_level: int = LOG_LEVEL) -> logging.Logger:
    """Configure logger with daily file and console output.

    Essential Features:
    - Creates daily log files with automatic directory creation
    - Prevents duplicate handlers for existing loggers
    - Structured formatting with script name, function, and line numbers
    - UTF-8 encoding support for cross-platform compatibility

    Args:
        script_name: Script identifier for log entries
        log_level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(script_name)

    if logger.handlers:
        return logger

    logger.setLevel(log_level)

    formatter = logging.Formatter(
        f"%(asctime)s - {script_name} - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt=DATE_FORMAT,
    )

    LOGS_DIR.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = LOGS_DIR / f"analystbot_{today}.log"
    log_file.touch(exist_ok=True)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logger initialized: {log_file}")
    return logger


def log_script_start(logger: logging.Logger, script_name: str, version: str = "3.1.0"):
    """Log script startup with key information.

    Essential Features:
    - Standardized startup banner format across all scripts
    - Version tracking for deployment monitoring
    - Clear visual separation in log files
    """
    logger.info("=" * 50)
    logger.info(f"STARTING {script_name.upper()} v{version}")
    logger.info("=" * 50)


def log_script_end(
    logger: logging.Logger, script_name: str, start_time: datetime, success: bool = True
):
    """Log script completion with duration and status.

    Essential Features:
    - Calculates and logs execution duration
    - Success/failure status tracking
    - Consistent completion logging format
    """
    duration = datetime.now() - start_time
    status = "COMPLETED" if success else "FAILED"
    logger.info(f"{script_name.upper()} {status} in {duration}")


def clean_old_logs(days_to_keep: int = 30):
    """Remove log files older than specified days.

    Essential Features:
    - Automatic old log file detection and cleanup
    - Date parsing from filename format (analystbot_YYYY-MM-DD.log)
    - Safe deletion with error handling for invalid filenames
    - Returns list of cleaned files for audit logging

    Args:
        days_to_keep: Days to retain logs (default: 30)

    Returns:
        List of deleted file names
    """
    if not LOGS_DIR.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    cleaned_files = []

    for log_file in LOGS_DIR.glob("analystbot_*.log"):
        try:
            date_str = log_file.stem.split("_", 1)[1]
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff_date:
                log_file.unlink()
                cleaned_files.append(log_file.name)
        except (ValueError, IndexError):
            continue

    return cleaned_files


def log_exceptions(logger: logging.Logger):
    """Decorator to automatically log function exceptions.

    Essential Features:
    - Automatic exception capture and logging with stack traces
    - Preserves original exception for proper error handling
    - Adds function name context to error logs
    - Re-raises exceptions to maintain normal error flow
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Exception in {func.__name__}: {e}", exc_info=True)
                raise

        return wrapper

    return decorator
