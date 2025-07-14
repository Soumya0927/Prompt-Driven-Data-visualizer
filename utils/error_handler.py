import logging
from typing import Dict, Any
import traceback

class ErrorHandler:
    """Handle and format errors for user-friendly display."""
    
    @staticmethod
    def handle_error(error: Exception) -> str:
        """Handle different types of errors and return user-friendly messages."""
        error_type = type(error).__name__
        error_message = str(error)
        
        # Database connection errors
        if "connection" in error_message.lower():
            return "❌ Database connection failed. Please check your connection settings."
        
        # SQL errors
        if "sql" in error_message.lower() or "syntax" in error_message.lower():
            return f"❌ SQL Error: {error_message}"
        
        # Authentication errors
        if "authentication" in error_message.lower() or "access denied" in error_message.lower():
            return "❌ Authentication failed. Please check your username and password."
        
        # Network errors
        if "network" in error_message.lower() or "timeout" in error_message.lower():
            return "❌ Network error. Please check your internet connection."
        
        # API errors
        if "api" in error_message.lower() or "groq" in error_message.lower():
            return "❌ AI service error. Please check your API key and try again."
        
        # Data processing errors
        if "dataframe" in error_message.lower() or "pandas" in error_message.lower():
            return "❌ Data processing error. Please check your data format."
        
        # Visualization errors
        if "chart" in error_message.lower() or "plot" in error_message.lower():
            return "❌ Chart generation error. The data might not be suitable for visualization."
        
        # Generic errors
        return f"❌ Error: {error_message}"
    
    @staticmethod
    def log_error(error: Exception, context: str = ""):
        """Log detailed error information."""
        logging.error(f"Error in {context}: {str(error)}")
        logging.error(f"Error type: {type(error).__name__}")
        logging.error(f"Traceback: {traceback.format_exc()}")
    
    @staticmethod
    def validate_input(data: Any, field_name: str, expected_type: type) -> bool:
        """Validate input data type."""
        if not isinstance(data, expected_type):
            raise ValueError(f"{field_name} must be of type {expected_type.__name__}")
        return True
    
    @staticmethod
    def safe_execute(func, *args, **kwargs) -> Dict[str, Any]:
        """Safely execute a function and return result or error."""
        try:
            result = func(*args, **kwargs)
            return {"success": True, "result": result}
        except Exception as e:
            error_msg = ErrorHandler.handle_error(e)
            ErrorHandler.log_error(e, func.__name__)
            return {"success": False, "error": error_msg}