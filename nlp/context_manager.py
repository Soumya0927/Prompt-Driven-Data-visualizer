from typing import Dict, Any, Optional
from .session_manager import SessionManager
import logging

class ContextManager:
    """Manages conversation context retrieval and relevance."""
    
    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager
    
    def get_relevant_context(self, query: str, follow_up_info: Dict[str, Any]) -> str:
        """Get relevant context for the query - let LLM decide relevance."""
        # Get only the last interaction
        history = self.session_manager.get_conversation_history(limit=1)
        
        if not history:
            return ""
        
        last_interaction = history[-1]
        
        # Format the last interaction context with proper details
        context_parts = []
        context_parts.append(f"Last interaction:")
        context_parts.append(f"User asked: '{last_interaction.query}'")
        context_parts.append(f"Response type: {last_interaction.response_type}")
        
        if last_interaction.sql_query:
            # Ensure SQL query is properly formatted and not truncated
            sql_query = last_interaction.sql_query.strip()
            if len(sql_query) > 200:  # If SQL is very long, truncate but show more context
                sql_query = sql_query[:200] + "..."
            context_parts.append(f"SQL executed: {sql_query}")
        
        if last_interaction.result_summary:
            context_parts.append(f"Result: {last_interaction.result_summary}")
        
        # Add timestamp for better context
        context_parts.append(f"Timestamp: {last_interaction.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return "\n".join(context_parts)
    
    def get_extended_context(self, query: str, limit: int = 3) -> str:
        """Get extended context with multiple interactions."""
        history = self.session_manager.get_conversation_history(limit=limit)
        
        if not history:
            return ""
        
        context_parts = []
        context_parts.append("Recent conversation history:")
        
        for i, interaction in enumerate(history, 1):
            context_parts.append(f"\n{i}. User asked: '{interaction.query}'")
            context_parts.append(f"   Response type: {interaction.response_type}")
            
            if interaction.sql_query:
                sql_preview = interaction.sql_query.strip()
                if len(sql_preview) > 100:
                    sql_preview = sql_preview[:100] + "..."
                context_parts.append(f"   SQL: {sql_preview}")
            
            if interaction.result_summary:
                context_parts.append(f"   Result: {interaction.result_summary}")
        
        return "\n".join(context_parts)
    
    def determine_context_usage(self, query: str, context: str, sql_query: str, llm) -> str:
        """Let LLM determine if and how context was used."""
        if not context:
            return "No previous context available"
        
        from langchain.chains import LLMChain
        from langchain_core.prompts import PromptTemplate
        
        usage_template = """
Analyze whether the previous conversation context was used to answer the current query.

Current User Query: {query}

Previous Context:
{context}

Generated SQL Query: {sql_query}

Task: Determine if the previous context influenced the SQL generation. Consider:
- Does the current query reference the previous interaction?
- Was the same table/data used?
- Are there follow-up patterns like "show more", "what about", "also show"?

If context was used, explain how in 1-2 sentences.
If context was not relevant, respond with "No previous context used".

Response:
"""
        
        try:
            prompt = PromptTemplate(
                input_variables=["query", "context", "sql_query"],
                template=usage_template
            )
            
            chain = LLMChain(llm=llm, prompt=prompt)
            usage_response = chain.run({
                "query": query,
                "context": context,
                "sql_query": sql_query
            })
            
            return usage_response.strip()
            
        except Exception as e:
            logging.warning(f"Failed to determine context usage: {str(e)}")
            return "Context analysis unavailable"
    
    def format_context_for_display(self, context: str, max_length: int = 100) -> str:
        """Format context for display in UI."""
        if not context:
            return ""
        
        if len(context) <= max_length:
            return context
        
        # Try to find a good break point
        truncated = context[:max_length]
        last_sentence = truncated.rfind('.')
        last_newline = truncated.rfind('\n')
        
        break_point = max(last_sentence, last_newline)
        if break_point > max_length * 0.7:  # If we found a good break point
            return context[:break_point + 1] + "..."
        else:
            return context[:max_length] + "..."
