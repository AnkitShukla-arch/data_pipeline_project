# monitoring/freshness_checker.py
import os
import time
import logging

DATA_PATH = "data/raw"  # Adjust to your actual ingestion path
FRESHNESS_THRESHOLD = 60 * 60 * 24  # 24 hours

def check_freshness():
    """Checks if the latest data file is fresh enough."""
    try:
        latest_file = None
        latest_mtime = 0

        for root, _, files in os.walk(DATA_PATH):
            for file in files:
                filepath = os.path.join(root, file)
                mtime = os.path.getmtime(filepath)
                if mtime > latest_mtime:
                    latest_file = filepath
                    latest_mtime = mtime

        if not latest_file:
            raise FileNotFoundError("No files found for freshness check.")

        age = time.time() - latest_mtime
        is_fresh = age < FRESHNESS_THRESHOLD

        logging.info(f"[Freshness Checker] Latest file: {latest_file}, Fresh: {is_fresh}")

        return {"file": latest_file, "is_fresh": is_fresh, "age_seconds": age}
    except Exception as e:
        logging.error(f"[Freshness Checker] Error: {str(e)}")
        raise

