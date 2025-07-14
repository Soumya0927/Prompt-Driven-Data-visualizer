import os
import re
from typing import Dict, Any
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
from langchain_groq import ChatGroq
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class QueryProcessor:
    """Process natural language queries and convert them to SQL or provide appropriate responses."""
    
    def __init__(self, db_connector, db_type: str):
        self.db_connector = db_connector
        self.db_type = db_type
        self.llm = ChatGroq(
            model_name="llama3-70b-8192",
            groq_api_key=os.getenv("GROQ_API_KEY"),
            temperature=0.1
        )
        
        # Define SQL generation prompt based on database type
        if db_type == "postgresql":
            self.sql_template = self._get_postgresql_template()
        elif db_type == "mysql":
            self.sql_template = self._get_mysql_template()
        elif db_type == "sqlite":
            self.sql_template = self._get_sqlite_template()
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _get_postgresql_template(self) -> str:
        return """
You are a PostgreSQL expert. Convert the user's question to a valid PostgreSQL SQL query.

Database Schema:
{schema}

Rules:
1. Only generate SELECT queries - no INSERT, UPDATE, DELETE, DROP, etc.
2. Use proper PostgreSQL syntax
3. Include LIMIT clause if not specified (default 100)
4. Use table and column names exactly as shown in schema
5. For aggregations, use appropriate GROUP BY clauses
6. Handle date/time columns properly
7. Use ILIKE for case-insensitive text searches
8. Return only the SQL query without explanation

User Question: {question}

SQL Query:
"""
    
    def _get_mysql_template(self) -> str:
        return """
You are a MySQL expert. Convert the user's question to a valid MySQL SQL query.

Database Schema:
{schema}

Rules:
1. Only generate SELECT queries - no INSERT, UPDATE, DELETE, DROP, etc.
2. Use proper MySQL syntax
3. Include LIMIT clause if not specified (default 100)
4. Use table and column names exactly as shown in schema
5. For aggregations, use appropriate GROUP BY clauses
6. Handle date/time columns properly
7. Use LIKE for text searches
8. Return only the SQL query without explanation

User Question: {question}

SQL Query:
"""
    def _get_sqlite_template(self) -> str:
        return """
        You are a SQLite expert. Convert the user's question to a valid SQLite SQL query.

        Database Schema:
        {schema}

        Rules:
        1. Only generate SELECT queries - no INSERT, UPDATE, DELETE, DROP, etc.
        2. Use proper SQLite syntax
        3. Include LIMIT clause if not specified (default 100)
        4. Use table and column names exactly as shown in schema
        5. For aggregations, use appropriate GROUP BY clauses
        6. Handle date/time columns properly
        7. Use LIKE for case-insensitive text searches
        8. Return only the SQL query without explanation

        User Question: {question}

        SQL Query:
        """
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process user query and return appropriate response."""
        try:
            # Classify query type
            query_type = self._classify_query(user_query)
            
            if query_type == "schema_request":
                return {
                    "type": "schema_query",
                    "content": "Here's your database schema information."
                }
            elif query_type == "table_list":
                return {
                    "type": "schema_query",
                    "content": "Here are all the tables in your database."
                }
            elif query_type == "data_query":
                return self._process_data_query(user_query)
            elif query_type == "general":
                return self._process_general_query(user_query)
            else:
                return {
                    "type": "general_response",
                    "content": "I'm not sure how to help with that. Try asking about your data or database schema."
                }
                
        except Exception as e:
            logging.error(f"Query processing failed: {str(e)}")
            raise
    
    def _classify_query(self, query: str) -> str:
        """Classify the type of query."""
        query_lower = query.lower()
        
        # Schema-related queries
        schema_keywords = ["schema", "table", "tables", "columns", "structure", "describe", "show tables"]
        if any(keyword in query_lower for keyword in schema_keywords):
            if "table" in query_lower and ("list" in query_lower or "show" in query_lower):
                return "table_list"
            return "schema_request"
        
        # Data queries
        data_keywords = ["select", "show", "find", "get", "count", "sum", "average", "max", "min", 
                        "top", "bottom", "chart", "graph", "plot", "visualize", "analyze"]
        if any(keyword in query_lower for keyword in data_keywords):
            return "data_query"
        
        # General queries
        return "general"
    
    def _process_data_query(self, query: str) -> Dict[str, Any]:
        """Process data-related queries and generate SQL."""
        try:
            # Get schema information
            schema_info = self.db_connector.get_schema_info()
            
            # Create prompt template
            prompt = PromptTemplate(
                input_variables=["question", "schema"],
                template=self.sql_template
            )
            
            # Generate SQL query
            chain = LLMChain(llm=self.llm, prompt=prompt)
            llm_response = chain.run({"question": query, "schema": schema_info})
            
            # Extract SQL from response
            sql_query = self._extract_sql(llm_response)
            
            # Validate SQL query
            if not self._validate_sql(sql_query):
                raise ValueError("Generated SQL query failed validation")
            
            return {
                "type": "data_query",
                "sql_query": sql_query,
                "explanation": f"I'll execute this query to answer your question: '{query}'"
            }
            
        except Exception as e:
            logging.error(f"Data query processing failed: {str(e)}")
            raise ValueError(f"Failed to process data query: {str(e)}")
    
    def _process_general_query(self, query: str) -> Dict[str, Any]:
        """Process general queries with AI assistance."""
        try:
            general_template = """
You are a helpful data analysis assistant. The user has asked a question about data analysis or databases.
Provide a helpful, informative response.

User Question: {question}

Response:
"""
            
            prompt = PromptTemplate(
                input_variables=["question"],
                template=general_template
            )
            
            chain = LLMChain(llm=self.llm, prompt=prompt)
            response = chain.run({"question": query})
            
            return {
                "type": "general_response",
                "content": response
            }
            
        except Exception as e:
            logging.error(f"General query processing failed: {str(e)}")
            return {
                "type": "general_response",
                "content": "I'm sorry, I couldn't process your request. Please try asking about your data or database schema."
            }
    
    def _extract_sql(self, text_response: str) -> str:
        """Extract SQL query from LLM response."""
        # Remove any markdown formatting
        text_response = text_response.strip()
        
        # Try to extract code block inside triple backticks
        match = re.search(r"```(?:sql)?\s*(.*?)\s*```", text_response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Try to find SQL query starting with SELECT
        lines = text_response.split('\n')
        sql_lines = []
        found_select = False
        
        for line in lines:
            line = line.strip()
            if line.upper().startswith('SELECT'):
                found_select = True
            if found_select:
                sql_lines.append(line)
                if line.endswith(';'):
                    break
        
        if sql_lines:
            return '\n'.join(sql_lines)
        
        # If no clear SQL found, try to extract from the entire response
        if 'SELECT' in text_response.upper():
            # Find the position of SELECT and extract from there
            select_pos = text_response.upper().find('SELECT')
            potential_sql = text_response[select_pos:].strip()
            
            # Try to find the end of the SQL query
            end_markers = [';', '\n\n', '```']
            for marker in end_markers:
                if marker in potential_sql:
                    potential_sql = potential_sql[:potential_sql.find(marker)]
                    break
            
            return potential_sql.strip()
        
        raise ValueError("No SQL query found in the LLM response")
    
    def _validate_sql(self, sql_query: str) -> bool:
        """Validate the generated SQL query."""
        if not sql_query:
            return False
        
        sql_upper = sql_query.upper().strip()
        
        # Must start with SELECT
        if not sql_upper.startswith('SELECT'):
            return False
        
        # Should not contain dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                return False
        
        # Basic syntax check
        if 'SELECT' not in sql_upper or 'FROM' not in sql_upper:
            return False
        
        return True
    
    def generate_explanation(self, sql_query: str, user_query: str) -> str:
        """Generate explanation for the SQL query."""
        try:
            explanation_template = """
Explain this SQL query in simple terms for a business user:

Original Question: {question}
SQL Query: {sql}

Explanation:
"""
            
            prompt = PromptTemplate(
                input_variables=["question", "sql"],
                template=explanation_template
            )
            
            chain = LLMChain(llm=self.llm, prompt=prompt)
            explanation = chain.run({"question": user_query, "sql": sql_query})
            
            return explanation
            
        except Exception as e:
            logging.error(f"Explanation generation failed: {str(e)}")
            return f"This query retrieves data to answer: '{user_query}'"