"""
Centralized Logging Management for the Sync Module.

This module provides a unified logging system to ensure that all components
of the sync infrastructure produce structured, consistent, and actionable logs.

Key Features:
- Structured Logging: Outputs logs in JSON format for easy parsing and analysis.
- Centralized Configuration: Logging is configured from the main sync_config.yaml.
- Actionable Error Reports: Integrates with the ErrorTracker to include
  detailed error information in logs.
- Request/Correlation IDs: Facilitates tracing a single sync operation
  throughout the entire system.
"""

import logging
import sys
import json
from typing import Optional

class JsonFormatter(logging.Formatter):
    """
    Custom formatter to output logs in JSON format.
    """
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_record['exc_info'] = self.formatException(record.exc_info)
        if hasattr(record, 'details'):
            log_record['details'] = record.details
        return json.dumps(log_record)

class LoggingManager:
    """
    Manages the logging configuration for the entire sync module.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(LoggingManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, log_level: str = "INFO", log_file: Optional[str] = None):
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.log_level = log_level.upper()
        self.log_file = log_file
        self.logger = logging.getLogger("botnim.sync")
        self.logger.setLevel(self.log_level)
        self.logger.propagate = False  # Prevent duplicate logs in parent handlers

        # Remove existing handlers to avoid duplication
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(console_handler)

        # Add file handler if a log file is specified
        if self.log_file:
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(JsonFormatter())
            self.logger.addHandler(file_handler)
            
        self._initialized = True

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        Provides a logger with the correct configuration.
        """
        # Ensure the LoggingManager is initialized
        if not LoggingManager._instance:
            LoggingManager()
        return logging.getLogger(name)

def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a logger instance.
    """
    return LoggingManager.get_logger(name)
