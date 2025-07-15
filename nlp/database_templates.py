
class DatabaseTemplates:
    """Database-specific SQL generation templates."""
    
    @staticmethod
    def get_postgresql_template() -> str:
        """Get PostgreSQL-specific SQL template."""
        return """
You are a PostgreSQL expert. Convert the user's question to a valid PostgreSQL SQL query.

Database Schema:
{schema}

Previous Conversation Context (use only if relevant to current query):
{context}

Rules:
1. Only generate SELECT queries - no INSERT, UPDATE, DELETE, DROP, etc.
2. Use proper PostgreSQL syntax
3. Include LIMIT clause if not specified (default 100)
4. Use table and column names exactly as shown in schema
5. For aggregations, use appropriate GROUP BY clauses
6. Handle date/time columns properly
7. Use ILIKE for case-insensitive text searches
8. **IMPORTANT**: Only use the previous context if it's directly relevant to answering the current question
9. If the current query is independent, ignore the previous context completely
10. Return only the SQL query without explanation

User Question: {question}

SQL Query:
"""
    
    @staticmethod
    def get_mysql_template() -> str:
        """Get MySQL-specific SQL template."""
        return """
You are a MySQL expert. Convert the user's question to a valid MySQL SQL query.

Database Schema:
{schema}

Previous Conversation Context (use only if relevant to current query):
{context}

Rules:
1. Only generate SELECT queries - no INSERT, UPDATE, DELETE, DROP, etc.
2. Use proper MySQL syntax
3. Include LIMIT clause if not specified (default 100)
4. Use table and column names exactly as shown in schema
5. For aggregations, use appropriate GROUP BY clauses
6. Handle date/time columns properly
7. Use LIKE for text searches
8. **IMPORTANT**: Only use the previous context if it's directly relevant to answering the current question
9. If the current query is independent, ignore the previous context completely
10. Return only the SQL query without explanation

User Question: {question}

SQL Query:
"""
    
    @staticmethod
    def get_sqlite_template() -> str:
        """Get SQLite-specific SQL template."""
        return """
You are a SQLite expert. Convert the user's question to a valid SQLite SQL query.

Database Schema:
{schema}

Previous Conversation Context (use only if relevant to current query):
{context}

Rules:
1. Only generate SELECT queries - no INSERT, UPDATE, DELETE, DROP, etc.
2. Use proper SQLite syntax
3. Include LIMIT clause if not specified (default 100)
4. Use table and column names exactly as shown in schema
5. For aggregations, use appropriate GROUP BY clauses
6. Handle date/time columns properly
7. Use LIKE for case-insensitive text searches
8. **IMPORTANT**: Only use the previous context if it's directly relevant to answering the current question
9. If the current query is independent, ignore the previous context completely
10. Return only the SQL query without explanation

User Question: {question}

SQL Query:
"""
    
    @staticmethod
    def get_template_for_database(db_type: str) -> str:
        """Get appropriate template for database type."""
        templates = {
            "postgresql": DatabaseTemplates.get_postgresql_template(),
            "mysql": DatabaseTemplates.get_mysql_template(),
            "sqlite": DatabaseTemplates.get_sqlite_template()
        }
        
        if db_type not in templates:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        return templates[db_type]
