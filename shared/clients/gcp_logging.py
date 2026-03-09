# GCP Logging client wrapper to integrate with Python's logging module and send logs to Google Cloud Logging

# Builtin imports
import logging

# Shared imports
from google.cloud import logging as gc_logging

class GCPLogger:
    __slots__ = ["client"]
    
    def __init__(self) -> None:
        self.client = None
        
    def enable_logging(self):
        self.client = gc_logging.Client()
        self.client.setup_logging()
    
    def disable_logging(self):
        logging.shutdown()
        
        if self.client:
            self.client.close()
    
    def log(self, level, message): logging.log(level, message)
    
    def info(self, message): logging.info(message)
    
    def error(self, message): logging.error(message)
    
    def critical(self, message): logging.critical(message)
    
    def __enter__(self):
        self.enable_logging()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable_logging()