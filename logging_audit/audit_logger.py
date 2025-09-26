import logging
import os

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def log_info(message: str):
    logging.info(message)
    print(f"✅ {message}")


def log_error(message: str):
    logging.error(message)
    print(f"❌ {message}")

