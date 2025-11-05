import logging
import os
import sys

def initlog(name: str = None) -> logging.Logger:
    """
    Initialize a logger that works inside and outside Airflow.
    If running inside Airflow, logs will go to the Airflow scheduler/webserver console.
    Otherwise, it falls back to a standard stream logger.
    """
    log = logging.getLogger(name or __name__)

    # Detect Airflow context
    in_airflow = "AIRFLOW_HOME" in os.environ or any(k.startswith("AIRFLOW__") for k in os.environ)
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log.setLevel(level)

    # --- If in Airflow, just use its handlers ---
    if in_airflow:
        # Airflow already has its own logging handlers configured
        if not log.handlers:
            airflow_handler = logging.StreamHandler(sys.stdout)
            airflow_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
            )
            airflow_handler.setFormatter(airflow_formatter)
            log.addHandler(airflow_handler)
        log.propagate = True  # Ensure Airflow captures it
        return log

    # --- Outside Airflow (e.g. FastAPI, Streamlit, CLI scripts) ---
    if not log.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        log.addHandler(console_handler)

    log.propagate = False
    return log
