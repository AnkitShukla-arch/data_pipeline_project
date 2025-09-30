
# monitoring/metrics_collector.py
import time
import psutil
import logging

def collect_metrics():
    """Collects system and pipeline metrics for monitoring."""
    try:
        metrics = {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage("/").percent,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        logging.info(f"[Metrics Collector] Collected metrics: {metrics}")
        return metrics
    except Exception as e:
        logging.error(f"[Metrics Collector] Failed to collect metrics: {str(e)}")
        raise
