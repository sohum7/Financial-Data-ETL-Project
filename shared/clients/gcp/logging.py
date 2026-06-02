# GCP Logging client wrapper to integrate with Python's logging module and send logs to Google Cloud Logging

# Built-in imports
import logging
from typing import Any

# Google API imports
from google.cloud import logging as gc_logging


# Custom GCP Logger
class CloudLogger:
    # Explicit slots for memory efficiency, completely safe now
    __slots__ = ["client", "_logger"]
    
    def __init__(self, logger_name: str = "gcp_cloud_logger") -> None:
        self.client: Any = None
        # Compose by holding a reference to a standard Python logger instance
        self._logger: logging.Logger = logging.getLogger(logger_name)
    
    @property
    def logger(self) -> logging.Logger:
        return self._logger
    
    @logger.setter
    def logger(self, value: logging.Logger) -> None:
        if not isinstance(value, logging.Logger):
            raise ValueError("logger must be an instance of logging.Logger")
        self._logger = value
    
    @logger.deleter
    def logger(self) -> None:
        del self._logger
    
    def enable_logging(self) -> None:
        # Avoid re-initializing if already active
        if not self.client:
            self.client = gc_logging.Client()
            self.client.setup_logging()
    
    def disable_logging(self) -> None:
        logging.shutdown()
        if self.client:
            # Check for close method safety depending on gc_logging version
            if hasattr(self.client, "close"):
                self.client.close()
            self.client = None
            
    # Delegate logging methods explicitly to the internal logger instance
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.info(msg, *args, **kwargs)
        
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.error(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._logger.warning(msg, *args, **kwargs)

    # Context manager implementation
    def __enter__(self) -> "CloudLogger":
        self.enable_logging()
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.disable_logging()
        return False  # Do not suppress exceptions

