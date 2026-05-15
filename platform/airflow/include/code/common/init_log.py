import logging
import os
import sys

def initlog(name: str = None) -> logging.Logger:

    log = logging.getLogger(name or __name__)

    in_airflow = (
        "AIRFLOW_HOME" in os.environ
        or any(k.startswith("AIRFLOW__") for k in os.environ)
    )

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log.setLevel(level)

    # -------------------------
    # Airflow mode
    # -------------------------
    if in_airflow:
        log.handlers.clear()
        # IMPORTANT:
        # Do NOT add StreamHandler here.
        # Airflow already manages handlers.
        log.propagate = True

        return log

    # -------------------------
    # Normal mode
    # -------------------------
    if not log.handlers:

        console_handler = logging.StreamHandler(sys.stdout)

        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )

        console_handler.setFormatter(console_formatter)

        log.addHandler(console_handler)

    log.propagate = False

    return log