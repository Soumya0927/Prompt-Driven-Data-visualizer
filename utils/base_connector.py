from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Optional

class BaseConnector(ABC):
    """Abstract base class for database connectors."""
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test database connection."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        pass
    
    @abstractmethod
    def get_schema_info(self) -> str:
        """Get comprehensive schema information."""
        pass
    
    @abstractmethod
    def get_table_sample(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample data from a table."""
        pass
    
    @abstractmethod
    def get_column_info(self, table_name: str) -> List[Dict]:
        """Get detailed column information for a table."""
        pass
    
    @abstractmethod
    def close(self):
        """Close database connection."""
        pass