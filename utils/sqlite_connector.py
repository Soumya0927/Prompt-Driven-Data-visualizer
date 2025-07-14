import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
import tempfile
import os
from typing import List, Dict, Optional, Union
from .base_connector import BaseConnector
import streamlit as st

class SQLiteConnector(BaseConnector):
    """SQLite database connector with CSV upload support and optimized queries."""
    
    def __init__(self, database_path: Optional[str] = None, uploaded_file=None):
        """
        Initialize SQLite connector.
        
        Args:
            database_path: Path to SQLite database file (for local files)
            uploaded_file: Streamlit uploaded file object (for uploaded .db files)
        """
        self.database_path = database_path
        self.uploaded_file = uploaded_file
        self.engine = None
        self.temp_db_path = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            if self.uploaded_file:
                # Handle uploaded database file
                self.temp_db_path = self._create_temp_db_from_upload()
                connection_string = f"sqlite:///{self.temp_db_path}"
            elif self.database_path:
                # Handle local database file
                connection_string = f"sqlite:///{self.database_path}"
            else:
                # Create temporary file database instead of in-memory
                self.temp_db_path = tempfile.NamedTemporaryFile(delete=False, suffix='.db').name
                connection_string = f"sqlite:///{self.temp_db_path}"
            
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            logging.info(f"Connected to SQLite database")
            
        except Exception as e:
            logging.error(f"Failed to connect to SQLite: {str(e)}")
            raise

    
    def _create_temp_db_from_upload(self) -> str:
        """Create temporary database file from uploaded file."""
        try:
            # Create temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            temp_file.write(self.uploaded_file.read())
            temp_file.close()
            
            # Reset file pointer for potential future use
            self.uploaded_file.seek(0)
            
            return temp_file.name
            
        except Exception as e:
            logging.error(f"Failed to create temporary database: {str(e)}")
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
            
            # Execute query
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
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            result = self.execute_query(query)
            return result['name'].tolist()
        except Exception as e:
            logging.error(f"Failed to get tables: {str(e)}")
            return []
    
    def get_schema_info(self) -> str:
        """Get comprehensive schema information."""
        try:
            tables = self.get_tables()
            
            if not tables:
                return "No tables found in the database."
            
            schema_info = ""
            
            for table in tables:
                schema_info += f"Table: {table}\n"
                
                # Quote table name to handle special characters
                quoted_table = f'"{table}"'
                query = f"PRAGMA table_info({quoted_table});"
                
                try:
                    result = self.execute_query(query)
                    
                    for _, row in result.iterrows():
                        column_name = row['name']
                        data_type = row['type']
                        not_null = "NOT NULL" if row['notnull'] else "NULL"
                        default_value = f" DEFAULT {row['dflt_value']}" if row['dflt_value'] else ""
                        primary_key = " (PK)" if row['pk'] else ""
                        
                        schema_info += f"  - {column_name}: {data_type}{primary_key} {not_null}{default_value}\n"
                    
                except Exception as table_error:
                    schema_info += f"  Error retrieving info for table {table}: {str(table_error)}\n"
                
                schema_info += "\n"
            
            return schema_info
            
        except Exception as e:
            return f"Schema information unavailable: {str(e)}"

    
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
            query = f"PRAGMA table_info({table_name});"
            result = self.execute_query(query)
            
            columns = []
            for _, row in result.iterrows():
                columns.append({
                    'column_name': row['name'],
                    'data_type': row['type'],
                    'is_nullable': 'YES' if not row['notnull'] else 'NO',
                    'column_default': row['dflt_value'],
                    'is_primary_key': bool(row['pk']),
                    'position': row['cid']
                })
            
            return columns
            
        except Exception as e:
            logging.error(f"Failed to get column info: {str(e)}")
            return []
    
    def create_table_from_csv(self, csv_file, table_name: str) -> bool:
        """Create a table from uploaded CSV file."""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Clean column names (replace spaces with underscores, remove special characters)
            df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('[^\w]', '', regex=True)
            
            # Remove any empty rows
            df = df.dropna(how='all')
            
            # Convert data types intelligently
            df = self._optimize_datatypes(df)
            
            # Create table in database
            df.to_sql(table_name, self.engine, index=False, if_exists='replace')
            
            logging.info(f"Created table '{table_name}' with {len(df)} rows and {len(df.columns)} columns")
            return True
            
        except Exception as e:
            logging.error(f"Failed to create table from CSV: {str(e)}")
            return False
    
    def _optimize_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize data types for better performance."""
        try:
            for column in df.columns:
                # Skip if column is already datetime
                if pd.api.types.is_datetime64_any_dtype(df[column]):
                    continue
                
                # Try to convert to numeric first
                if df[column].dtype == 'object':
                    # Try to convert to numeric
                    numeric_series = pd.to_numeric(df[column], errors='coerce')
                    if not numeric_series.isna().all():
                        # Check if it's integer or float
                        if numeric_series.notna().all() and (numeric_series % 1 == 0).all():
                            df[column] = numeric_series.astype('Int64')  # Nullable integer
                        else:
                            df[column] = numeric_series
                    else:
                        # Try to convert to datetime
                        try:
                            datetime_series = pd.to_datetime(df[column], errors='coerce')
                            if not datetime_series.isna().all():
                                df[column] = datetime_series
                        except:
                            pass  # Keep as string
            
            return df
            
        except Exception as e:
            logging.error(f"Failed to optimize datatypes: {str(e)}")
            return df
    
    def get_table_stats(self, table_name: str) -> Dict:
        """Get basic statistics for a table."""
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name};"
            row_count = self.execute_query(count_query).iloc[0]['row_count']
            
            # Get column count
            columns = self.get_column_info(table_name)
            column_count = len(columns)
            
            # Get sample data to analyze
            sample_data = self.get_table_sample(table_name, 1000)
            
            stats = {
                'row_count': row_count,
                'column_count': column_count,
                'columns': [col['column_name'] for col in columns],
                'sample_size': len(sample_data)
            }
            
            # Add basic stats for numeric columns
            numeric_columns = sample_data.select_dtypes(include=['number']).columns
            if len(numeric_columns) > 0:
                stats['numeric_columns'] = list(numeric_columns)
            
            return stats
            
        except Exception as e:
            logging.error(f"Failed to get table stats: {str(e)}")
            return {}
    
    def close(self):
        """Close database connection and cleanup temporary files."""
        if self.engine:
            self.engine.dispose()
            logging.info("SQLite connection closed")
        
        # Clean up temporary database file if it exists
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            try:
                os.unlink(self.temp_db_path)
                logging.info("Temporary database file cleaned up")
            except Exception as e:
                logging.warning(f"Failed to cleanup temporary file: {str(e)}")
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        self.close()