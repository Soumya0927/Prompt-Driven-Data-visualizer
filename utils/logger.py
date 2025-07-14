import logging
import os
from datetime import datetime
from typing import Optional

class Logger:
    """Centralized logging utility."""
    
    _logger = None
    
    @classmethod
    def get_logger(cls, name: str = "dataviz_chatbot") -> logging.Logger:
        """Get or create logger instance."""
        if cls._logger is None:
            cls._logger = logging.getLogger(name)
            cls._setup_logger()
        return cls._logger
    
    @classmethod
    def _setup_logger(cls):
        """Setup logger configuration."""
        # Create logs directory if it doesn't exist
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Set log level
        cls._logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # File handler
        log_file = os.path.join(log_dir, f"app_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        cls._logger.addHandler(file_handler)
        cls._logger.addHandler(console_handler)
    
    @classmethod
    def log_info(cls, message: str):
        """Log info message."""
        logger = cls.get_logger()
        logger.info(message)
    
    @classmethod
    def log_warning(cls, message: str):
        """Log warning message."""
        logger = cls.get_logger()
        logger.warning(message)
    
    @classmethod
    def log_error(cls, message: str):
        """Log error message."""
        logger = cls.get_logger()
        logger.error(message)
    
    @classmethod
    def log_debug(cls, message: str):
        """Log debug message."""
        logger = cls.get_logger()
        logger.debug(message)