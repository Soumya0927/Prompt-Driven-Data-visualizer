import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import List, Dict, Optional
from .base_connector import BaseConnector

class PostgreSQLConnector(BaseConnector):
    """PostgreSQL database connector with optimized queries and error handling."""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        self.engine = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string, pool_pre_ping=True)
            logging.info(f"Connected to PostgreSQL database: {self.database}")
        except Exception as e:
            logging.error(f"Failed to connect to PostgreSQL: {str(e)}")
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
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
            """
            result = self.execute_query(query)
            return result['table_name'].tolist()
        except Exception as e:
            logging.error(f"Failed to get tables: {str(e)}")
            return []
    
    def get_schema_info(self) -> str:
        """Get comprehensive schema information."""
        try:
            query = """
            SELECT 
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                CASE 
                    WHEN pk.column_name IS NOT NULL THEN 'PK'
                    WHEN fk.column_name IS NOT NULL THEN 'FK'
                    ELSE ''
                END as key_type
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
            LEFT JOIN (
                SELECT ku.table_name, ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku 
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
            LEFT JOIN (
                SELECT ku.table_name, ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku 
                    ON tc.constraint_name = ku.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
            ) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name
            WHERE t.table_schema = 'public'
            ORDER BY t.table_name, c.ordinal_position;
            """
            
            result = self.execute_query(query)
            
            # Format schema info
            schema_info = ""
            current_table = None
            
            for _, row in result.iterrows():
                if current_table != row['table_name']:
                    if current_table is not None:
                        schema_info += "\n"
                    current_table = row['table_name']
                    schema_info += f"Table: {current_table}\n"
                
                key_info = f" ({row['key_type']})" if row['key_type'] else ""
                nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {row['column_default']}" if row['column_default'] else ""
                
                schema_info += f"  - {row['column_name']}: {row['data_type']}{key_info} {nullable}{default}\n"
            
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
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position;
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
            logging.info("PostgreSQL connection closed")