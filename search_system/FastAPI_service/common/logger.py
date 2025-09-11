import logging
import sys
import os

class CustomError(Exception):
    """A custom exception class."""
    pass

# def initlog(logName):
#     logger = logging.getLogger(logName)
#     if not logger.handlers:  
#         # Ensure the logs directory exists
#         os.makedirs('./logs', exist_ok=True)
#         logFile = f'./logs/{logName}.log'
        
#         # File handler for logging to a file
#         file_handler = logging.FileHandler(logFile, encoding='UTF-8')
#         file_formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s')
#         file_handler.setFormatter(file_formatter)
        
#         # Stream handler for logging to console
#         console_handler = logging.StreamHandler(sys.stdout)
#         console_formatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s')
#         console_handler.setFormatter(console_formatter) 
        
#         # Add handlers to the logger
#         logger.addHandler(file_handler)
#         logger.addHandler(console_handler)
        
#         # Set the logger level
#         logger.setLevel(logging.INFO)
    
#     return logger


def initlog(name: str | None = None) -> logging.Logger:
    log = logging.getLogger(name)
    if any(h for h in log.handlers):
        # Airflow (and uvicorn, gunicorn) usually pre-configure handlers.
        return log

    # Outside Airflow: add a basic console handler
    in_airflow = "AIRFLOW_HOME" in os.environ or any(k.startswith("AIRFLOW__") for k in os.environ)
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log.setLevel(level)

    if not in_airflow:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        log.addHandler(ch)

    return log
