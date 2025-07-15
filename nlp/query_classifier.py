class QueryClassifier:
    """Classifies user queries into different types."""
    
    @staticmethod
    def classify_query(query: str) -> str:
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
    
    @staticmethod
    def is_data_query(query: str) -> bool:
        """Check if the query is asking for data."""
        return QueryClassifier.classify_query(query) == "data_query"
    
    @staticmethod
    def is_schema_query(query: str) -> bool:
        """Check if the query is asking for schema information."""
        query_type = QueryClassifier.classify_query(query)
        return query_type in ["schema_request", "table_list"]
