import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import List, Dict, Optional
from .base_connector import BaseConnector

class MySQLConnector(BaseConnector):
    """MySQL database connector with optimized queries and error handling."""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        self.engine = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string, pool_pre_ping=True)
            logging.info(f"Connected to MySQL database: {self.database}")
        except Exception as e:
            logging.error(f"Failed to connect to MySQL: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logging.error(f"Connection test failed: {str(e)}")
            return False
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            # Clean and validate query
            query = query.strip()
            if not query:
                raise ValueError("Empty query provided")
            
            # Security check - prevent dangerous operations
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
            query_upper = query.upper()
            
            for keyword in dangerous_keywords:
                if keyword in query_upper and not query_upper.startswith('SELECT'):
                    raise ValueError(f"Dangerous operation detected: {keyword}")
            
            # Execute query with timeout
            with self.engine.connect() as conn:
                result = pd.read_sql_query(text(query), conn)
                
            logging.info(f"Query executed successfully: {query[:100]}...")
            return result
            
        except SQLAlchemyError as e:
            logging.error(f"SQL execution error: {str(e)}")
            raise ValueError(f"SQL Error: {str(e)}")
        except Exception as e:
            logging.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        try:
            query = f"SHOW TABLES;"
            result = self.execute_query(query)
            # MySQL returns tables in a column named 'Tables_in_{database_name}'
            table_column = f"Tables_in_{self.database}"
            return result[table_column].tolist()
        except Exception as e:
            logging.error(f"Failed to get tables: {str(e)}")
            return []
    
    def get_schema_info(self) -> str:
        """Get comprehensive schema information."""
        try:
            query = f"""
            SELECT 
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMN_KEY,
                EXTRA,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{self.database}'
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
            """
            
            result = self.execute_query(query)
            
            # Format schema info
            schema_info = ""
            current_table = None
            
            for _, row in result.iterrows():
                if current_table != row['TABLE_NAME']:
                    if current_table is not None:
                        schema_info += "\n"
                    current_table = row['TABLE_NAME']
                    schema_info += f"Table: {current_table}\n"
                
                key_info = ""
                if row['COLUMN_KEY'] == 'PRI':
                    key_info = " (PK)"
                elif row['COLUMN_KEY'] == 'MUL':
                    key_info = " (FK)"
                elif row['COLUMN_KEY'] == 'UNI':
                    key_info = " (UNIQUE)"
                
                nullable = "NULL" if row['IS_NULLABLE'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {row['COLUMN_DEFAULT']}" if row['COLUMN_DEFAULT'] else ""
                extra = f" {row['EXTRA']}" if row['EXTRA'] else ""
                
                data_type = row['DATA_TYPE']
                if row['CHARACTER_MAXIMUM_LENGTH']:
                    data_type += f"({row['CHARACTER_MAXIMUM_LENGTH']})"
                elif row['NUMERIC_PRECISION']:
                    if row['NUMERIC_SCALE']:
                        data_type += f"({row['NUMERIC_PRECISION']},{row['NUMERIC_SCALE']})"
                    else:
                        data_type += f"({row['NUMERIC_PRECISION']})"
                
                schema_info += f"  - {row['COLUMN_NAME']}: {data_type}{key_info} {nullable}{default}{extra}\n"
            
            return schema_info
            
        except Exception as e:
            logging.error(f"Failed to get schema info: {str(e)}")
            return "Schema information unavailable"
    
    def get_table_sample(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample data from a table."""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit};"
            return self.execute_query(query)
        except Exception as e:
            logging.error(f"Failed to get table sample: {str(e)}")
            return pd.DataFrame()
    
    def get_column_info(self, table_name: str) -> List[Dict]:
        """Get detailed column information for a table."""
        try:
            query = f"""
            SELECT 
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                NUMERIC_PRECISION as numeric_precision,
                NUMERIC_SCALE as numeric_scale,
                COLUMN_KEY as column_key,
                EXTRA as extra
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{self.database}' 
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION;
            """
            result = self.execute_query(query)
            return result.to_dict('records')
        except Exception as e:
            logging.error(f"Failed to get column info: {str(e)}")
            return []
    
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logging.info("MySQL connection closed")