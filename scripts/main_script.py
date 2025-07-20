#!/usr/bin/env python3
"""
version 3.1.0
Main Orchestrator Script

Key Features:
- Sequential execution of data pipeline: update_db.py → calculate_indicators.py → send_telegram.py
- Comprehensive error handling with critical vs non-critical script classification
- Automated log cleanup and daily log rotation management
- Telegram completion summaries with success rates and failure details
- Subprocess timeout handling and return code validation
- Prerequisites validation (database, .env file, script dependencies)

Executes the complete analyst bot workflow: data fetching, indicator calculation, and Telegram messaging.
Manages script dependencies, error handling, and completion reporting for automated daily execution.
"""

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Import centralized logging
from logging_config import (
    setup_logger,
    log_script_start,
    log_script_end,
    clean_old_logs,
)

# Setup logging
logger = setup_logger("main_script")

# Get project paths
PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

# Script execution order
EXECUTION_ORDER = [
    {
        "name": "Update Database Script",
        "script": "update_db.py",
        "description": "Updates OHLCV data from Polygon.io into database",
        "timeout": None,  # No timeout - can take as long as needed
        "critical": True,
    },
    {
        "name": "Calculate Indicators Script",
        "script": "calculate_indicators.py",
        "description": "Calculates indicators from OHLCV data in database",
        "timeout": 1800,  # 30 minutes
        "critical": True,
    },
    {
        "name": "Send Telegram Messages Script",
        "script": "send_telegram.py",
        "description": "Requests data from database, generates watchlists, and sends to Telegram",
        "timeout": 600,  # 10 minutes
        "critical": False,  # Non-critical - don't stop if this fails
    },
]


def setup_logging():
    """Initialize logging and clean old log files.

    Essential Features:
    - Centralized logging setup using logging_config module
    - Automatic cleanup of logs older than 30 days
    - Startup banner and initialization confirmation
    """
    log_script_start(logger, "Main Update Script")

    cleaned_files = clean_old_logs(days_to_keep=30)
    if cleaned_files:
        logger.info(f"Cleaned {len(cleaned_files)} old log files")
    else:
        logger.info("No old logs to clean")


def run_script(script_info):
    """Execute single script with timeout and error handling.

    Essential Features:
    - Subprocess execution with timeout protection
    - Return code validation and error classification
    - Critical vs non-critical failure handling
    - Execution duration tracking and logging
    - File existence validation before execution
    """
    script_name = script_info["name"]
    script_file = script_info["script"]
    timeout = script_info["timeout"]
    critical = script_info["critical"]

    script_path = SCRIPTS_DIR / script_file

    logger.info(f"Starting {script_name}")

    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return not critical

    start_time = datetime.now()

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,
            timeout=timeout,
        )

        duration = datetime.now() - start_time

        if result.returncode == 0:
            logger.info(f"✅ {script_name} completed in {duration}")
            return True
        else:
            logger.error(f"❌ {script_name} failed (code {result.returncode})")
            return not critical

    except subprocess.TimeoutExpired:
        logger.error(f"❌ {script_name} timed out after {timeout}s")
        return not critical
    except Exception as e:
        logger.error(f"❌ {script_name} exception: {e}")
        return not critical


def check_prerequisites():
    """Validate system requirements and file dependencies.

    Essential Features:
    - Automatic data directory creation if missing
    - .env file existence check with required variable guidance
    - Script dependency validation across all execution steps
    - Clear error messaging for missing components
    """
    logger.info("Checking prerequisites")

    db_dir = PROJECT_ROOT / "data"
    if not db_dir.exists():
        logger.info("Creating data directory")
        db_dir.mkdir(parents=True, exist_ok=True)

    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        logger.warning("Missing .env file - create with:")
        logger.warning("  POLYGON_KEY=your_polygon_api_key")
        logger.warning("  FRED_API=your_fred_api_key")
        logger.warning("  TELEGRAM_BOT_TOKEN=your_bot_token")
        logger.warning("  TELEGRAM_CHAT_ID=your_chat_id")
        logger.warning("  TELEGRAM_TEST_CHAT_ID=your_test_chat_id")
        logger.warning("  TELEGRAM_CHAT_BTD_ID=your_btd_topic_id")
        logger.warning("  TELEGRAM_CHAT_STR_ID=your_str_topic_id")
        logger.warning(
            "  TELEGRAM_CHAT_MARKET_INDICATORS_ID=your_market_indicators_topic_id"
        )

    missing_scripts = [
        script_info["script"]
        for script_info in EXECUTION_ORDER
        if not (SCRIPTS_DIR / script_info["script"]).exists()
    ]

    if missing_scripts:
        logger.error(f"Missing scripts: {missing_scripts}")
        return False

    logger.info("✅ Prerequisites validated")
    return True


def send_completion_summary(
    start_time, end_time, success_count, total_count, failed_scripts
):
    """Send execution summary to Telegram.

    Essential Features:
    - Calculates success rate and total execution duration
    - Status emoji selection based on completion rate
    - Failed script listing for troubleshooting
    - Formatted timestamp and duration display
    """
    try:
        from send_telegram import send_telegram_message

        duration = end_time - start_time
        success_rate = (success_count / total_count) * 100

        status = (
            "✅ SUCCESS"
            if success_count == total_count
            else "⚠️ PARTIAL" if success_count > 0 else "❌ FAILED"
        )

        message = f"{status}\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n⏱️ {duration}\n📊 {success_rate:.1f}% ({success_count}/{total_count})"

        if failed_scripts:
            message += f"\n❌ Failed: {', '.join(failed_scripts)}"

        send_telegram_message(message)
        logger.info("Summary sent to Telegram")
    except Exception as e:
        logger.error(f"Failed to send summary: {e}")


def cleanup_old_logs():
    """Clean up old log files (standalone function).

    Essential Features:
    - Standalone log cleanup for manual execution
    - 30-day retention policy with deletion confirmation
    - Audit logging of cleanup operations
    """
    logger.info("Starting log cleanup")
    cleaned_files = clean_old_logs(days_to_keep=30)
    if cleaned_files:
        logger.info(f"Deleted {len(cleaned_files)} old log files")
    else:
        logger.info("No old logs to clean")


def main():
    """Execute complete analyst bot workflow.

    Essential Features:
    - Sequential script execution with dependency management
    - Comprehensive error handling and failure recovery
    - Success rate calculation and completion reporting
    - Exit code management for cron job integration
    - Telegram notification of overall process status
    """
    setup_logging()
    start_time = datetime.now()

    logger.info(f"Processing {len(EXECUTION_ORDER)} scripts")

    if not check_prerequisites():
        logger.error("Prerequisites failed")
        sys.exit(1)

    success_count = 0
    failed_scripts = []

    for i, script_info in enumerate(EXECUTION_ORDER, 1):
        logger.info(f"[{i}/{len(EXECUTION_ORDER)}] {script_info['name']}")

        if run_script(script_info):
            success_count += 1
        else:
            failed_scripts.append(script_info["name"])
            if script_info["critical"]:
                logger.error(f"Critical failure: {script_info['name']}")
                break

    end_time = datetime.now()

    logger.info(f"Completed: {success_count}/{len(EXECUTION_ORDER)} successful")
    if failed_scripts:
        logger.info(f"Failed: {', '.join(failed_scripts)}")

    send_completion_summary(
        start_time, end_time, success_count, len(EXECUTION_ORDER), failed_scripts
    )

    success = success_count == len(EXECUTION_ORDER)
    log_script_end(logger, "Main Update Script", start_time, success)

    if success_count == len(EXECUTION_ORDER):
        logger.info("✅ All scripts completed")
        sys.exit(0)
    elif success_count > 0:
        logger.warning("⚠️ Partial completion")
        sys.exit(1)
    else:
        logger.error("❌ All scripts failed")
        sys.exit(2)


if __name__ == "__main__":
    main()
