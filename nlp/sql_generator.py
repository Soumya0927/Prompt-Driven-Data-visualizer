import re
from typing import Dict, Any
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
import logging

class SQLGenerator:
    """Handles SQL generation and validation."""
    
    def __init__(self, llm, db_connector):
        self.llm = llm
        self.db_connector = db_connector
    
    def generate_sql(self, query: str, context: str, sql_template: str) -> str:
        """Generate SQL query using the provided template."""
        try:
            # Get schema information
            schema_info = self.db_connector.get_schema_info()
            
            # Create prompt template
            prompt = PromptTemplate(
                input_variables=["question", "schema", "context"],
                template=sql_template
            )
            
            # Generate SQL query
            chain = LLMChain(llm=self.llm, prompt=prompt)
            llm_response = chain.run({
                "question": query,
                "schema": schema_info,
                "context": context
            })
            
            # Extract SQL from response
            sql_query = self._extract_sql(llm_response)
            
            # Validate SQL query
            if not self._validate_sql(sql_query):
                raise ValueError("Generated SQL query failed validation")
            
            return sql_query
            
        except Exception as e:
            logging.error(f"SQL generation failed: {str(e)}")
            raise ValueError(f"Failed to generate SQL query: {str(e)}")
    
    def _extract_sql(self, text_response: str) -> str:
        """Extract SQL query from LLM response."""
        # Remove any markdown formatting
        text_response = text_response.strip()
        
        # Try to extract code block inside triple backticks
        match = re.search(r"``````", text_response, re.DOTALL | re.IGNORECASE)
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
