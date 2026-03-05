from google.cloud import logging as google_cloud_logging
import logging

# Setup the library to work with standard Python logging
# This "patches" the root logger to send logs to GCP

class GCPLogger:
    __slots__ = ["name", "client", "logger"]
    
    def __init__(self, name="GCP LOGGER") -> None:
        self.name = name
        
    def enable_logging(self):
        self.client = google_cloud_logging.Client()
        self.client.setup_logging()
        self.logger = logging.getLogger(self.name)
    
    def disable_logging(self):
        logging.shutdown()
        
        if self.client:
            self.client.close()
    
    def log(self, level, message): self.logger.log(level, message)
    
    def info(self, message): self.logger.info(message)
    
    def error(self, message): self.logger.error(message)
    
    def critical(self, message): self.logger.critical(message)
    
    def __enter__(self):
        self.enable_logging()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable_logging()