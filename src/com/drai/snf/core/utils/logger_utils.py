# src/com/dataready_ai/snf/core/utils/logger_utils.py
import logging
import functools
import os

# Singleton default logger
DEFAULT_LOGGER = None

def get_logger(name: str = "default", log_file: str = None) -> logging.Logger:
    """
    Returns a singleton logger.
    If called multiple times, returns the same logger instance.
    """
    global DEFAULT_LOGGER
    if DEFAULT_LOGGER:
        return DEFAULT_LOGGER

    logger = logging.getLogger(name)
    
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # File handler if log_file is provided
        if log_file:
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.INFO)
            logger.addHandler(fh)
            print(f"Logs will be written to: {os.path.abspath(log_file)}")

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # Log format
        formatter = logging.Formatter(
            '%(asctime)s | %(module)s | %(levelname)s | %(message)s'
        )
        ch.setFormatter(formatter)
        if log_file:
            fh.setFormatter(formatter)

        logger.addHandler(ch)

    DEFAULT_LOGGER = logger
    return logger


def log_execution(logger: logging.Logger = None):
    """
    Decorator to log function/method entry, exit, arguments, return values, and exceptions.
    If no logger is passed, uses the default singleton logger.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal logger
            from .logger_utils import get_logger
            if logger is None:
                logger = get_logger(func.__module__)
            func_name = func.__qualname__

            # Log function entry
            logger.info(f"Entering: {func_name} | args={args} kwargs={kwargs}")
            try:
                result = func(*args, **kwargs)
                # Log function exit
                logger.info(f"Exiting : {func_name} | Returned: {result}")
                return result
            except Exception as e:
                logger.exception(f"Exception in {func_name}: {e}")
                raise
        return wrapper
    return decorator
