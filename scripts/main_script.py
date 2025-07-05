#!/usr/bin/env python3
"""
version 3.0.0
Main Update Script
Orchestrates all data updates calculations and messaging in proper sequence
Optimised for cron job execution on Raspberry Pi
"""

import os
import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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
    """Setup simple console logging"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger.info("Console logging initialized")


def run_script(script_info):
    """Run a single script with error handling and timeout"""
    script_name = script_info["name"]
    script_file = script_info["script"]
    timeout = script_info["timeout"]
    critical = script_info["critical"]

    script_path = SCRIPTS_DIR / script_file

    logger.info(f"Starting {script_name}...")
    logger.info(f"Script: {script_path}")
    logger.info(f"Description: {script_info['description']}")

    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        if critical:
            return False
        else:
            logger.warning(f"Non-critical script missing, continuing...")
            return True

    start_time = datetime.now()

    try:
        # Run the script - output goes directly to console
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,
            timeout=timeout,  # None means no timeout
        )

        end_time = datetime.now()
        duration = end_time - start_time

        if result.returncode == 0:
            logger.info(f"✅ {script_name} completed successfully in {duration}")
            return True
        else:
            logger.error(
                f"❌ {script_name} failed with return code {result.returncode}"
            )
            if critical:
                return False
            else:
                logger.warning(f"Non-critical script failed, continuing...")
                return True

    except subprocess.TimeoutExpired:
        timeout_msg = f"after {timeout} seconds" if timeout else "with no timeout set"
        logger.error(f"❌ {script_name} timed out {timeout_msg}")
        if critical:
            return False
        else:
            logger.warning(f"Non-critical script timed out, continuing...")
            return True

    except Exception as e:
        logger.error(f"❌ {script_name} failed with exception: {e}")
        if critical:
            return False
        else:
            logger.warning(f"Non-critical script failed, continuing...")
            return True


def check_prerequisites():
    """Check if all prerequisites are met"""
    logger.info("Checking prerequisites...")

    # Check if database directory exists
    db_dir = PROJECT_ROOT / "data"
    if not db_dir.exists():
        logger.warning(f"Database directory not found: {db_dir}")
        logger.info("Creating database directory...")
        db_dir.mkdir(parents=True, exist_ok=True)

    # Check if .env file exists
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        logger.warning(f"Environment file not found: {env_file}")
        logger.warning(
            "Create .env file with POLYGON_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID"
        )

    # Check if all scripts exist
    missing_scripts = []
    for script_info in EXECUTION_ORDER:
        script_path = SCRIPTS_DIR / script_info["script"]
        if not script_path.exists():
            missing_scripts.append(script_info["script"])

    if missing_scripts:
        logger.error(f"Missing required scripts: {missing_scripts}")
        return False

    logger.info("✅ All prerequisites check passed")
    return True


def send_completion_summary(
    start_time, end_time, success_count, total_count, failed_scripts
):
    """Send completion summary via Telegram"""
    try:
        duration = end_time - start_time
        success_rate = (success_count / total_count) * 100

        # Import here to avoid circular imports
        from send_telegram import send_telegram_message

        if success_count == total_count:
            status_emoji = "✅"
            status_text = "SUCCESS"
        elif success_count > 0:
            status_emoji = "⚠️"
            status_text = "PARTIAL"
        else:
            status_emoji = "❌"
            status_text = "FAILED"

        message = f"{status_emoji} <b>Daily Update {status_text}</b>\n"
        message += f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        message += f"⏱️ Duration: {duration}\n"
        message += (
            f"📊 Success Rate: {success_rate:.1f}% ({success_count}/{total_count})\n"
        )

        if failed_scripts:
            message += f"\n❌ Failed Scripts:\n"
            for script in failed_scripts:
                message += f"   • {script}\n"

        message += f"\n💡 Check logs for detailed information"

        # send_telegram_message(message)
        logger.info("Completion summary sent to Telegram")

    except Exception as e:
        logger.error(f"Failed to send completion summary: {e}")


def main():
    """Main orchestrator function"""
    setup_logging()

    logger.info("=" * 60)
    logger.info("STARTING DAILY UPDATE PROCESS")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Project Root: {PROJECT_ROOT}")
    logger.info(f"Scripts Directory: {SCRIPTS_DIR}")
    logger.info(f"Total Scripts: {len(EXECUTION_ORDER)}")

    start_time = datetime.now()

    # Check prerequisites
    if not check_prerequisites():
        logger.error("Prerequisites check failed. Exiting...")
        sys.exit(1)

    # Execute scripts in order
    success_count = 0
    failed_scripts = []

    for i, script_info in enumerate(EXECUTION_ORDER, 1):
        logger.info(f"\n[{i}/{len(EXECUTION_ORDER)}] Executing: {script_info['name']}")

        if run_script(script_info):
            success_count += 1
        else:
            failed_scripts.append(script_info["name"])
            if script_info["critical"]:
                logger.error(f"Critical script failed: {script_info['name']}")
                logger.error("Stopping execution due to critical failure")
                break

    end_time = datetime.now()
    total_duration = end_time - start_time

    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("DAILY UPDATE PROCESS COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Total Duration: {total_duration}")
    logger.info(f"Successful Scripts: {success_count}/{len(EXECUTION_ORDER)}")
    logger.info(f"Failed Scripts: {len(failed_scripts)}")

    if failed_scripts:
        logger.info(f"Failed Scripts: {', '.join(failed_scripts)}")

    # Send completion summary
    send_completion_summary(
        start_time, end_time, success_count, len(EXECUTION_ORDER), failed_scripts
    )

    # Exit with appropriate code
    if success_count == len(EXECUTION_ORDER):
        logger.info("✅ All scripts completed successfully")
        sys.exit(0)
    elif success_count > 0:
        logger.warning("⚠️ Some scripts failed, but process partially completed")
        sys.exit(1)
    else:
        logger.error("❌ All scripts failed")
        sys.exit(2)


if __name__ == "__main__":
    main()
