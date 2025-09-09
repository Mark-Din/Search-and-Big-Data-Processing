import logging
from logging.handlers import RotatingFileHandler

def manage_logs(log_file):
    """
    Configures log rotation for the cron log file.
    """
    logger = logging.getLogger('cron_log_manager')
    if not logger.handlers:
        # Set up RotatingFileHandler
        handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,  # Keep 5 backup files
            encoding='UTF-8'
        )
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger

if __name__ == "__main__":
    log_file_path = '/opt/penta_QR/python/code/cron_log/cron.log'
    logger = manage_logs(log_file_path)
    logger.info("Log rotation initialized.")
