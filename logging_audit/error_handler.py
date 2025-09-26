import time
from functools import wraps
import logging

class RetryableError(Exception):
    """Custom exception for retryable pipeline errors."""
    pass

def retry(max_attempts=3, delay=2, logger: logging.Logger = None):
    """
    Retry decorator for transient pipeline failures.
    
    Example usage:
        @retry(max_attempts=5, delay=3, logger=my_logger)
        def load_to_warehouse():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except RetryableError as e:
                    attempts += 1
                    if logger:
                        logger.warning(
                            f"RetryableError: {e}. Attempt {attempts}/{max_attempts}"
                        )
                    if attempts >= max_attempts:
                        if logger:
                            logger.error("Max retry attempts reached. Failing...")
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator

