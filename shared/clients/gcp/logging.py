# GCP Logging client wrapper to integrate with Python's logging module and send logs to Google Cloud Logging

# Built-in imports
import logging

# Google API imports
from google.cloud import logging as gc_logging


# Custom GCP Logger
class CloudLogger(logging.Logger):
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
    
    # Proper enter handling of connection creations within context managers
    def __enter__(self):
        self.enable_logging()
        return self
    
    # Proper exit handling of connection creations within context managers
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable_logging()
        return False # dont suppress exceptions

