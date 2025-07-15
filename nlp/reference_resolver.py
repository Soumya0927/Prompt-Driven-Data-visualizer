from typing import Dict, Any
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
import logging

class ReferenceResolver:
    """Resolves references and pronouns in user queries."""
    
    def __init__(self, llm):
        self.llm = llm
    
    def resolve_query_references(self, query: str, context: str) -> str:
        """Let LLM resolve query references instead of hardcoding patterns."""
        if not context:
            return query
        
        # Use LLM to resolve references
        resolution_template = """
You are helping to resolve references in a user query based on conversation context.

Previous Context:
{context}

Current User Query: {query}

Task: If the current query references the previous interaction (using words like "that", "it", "this", "previous", "last", etc.), 
provide a resolved version that makes the reference explicit. If the query is independent and doesn't reference 
the previous context, return the original query unchanged.

Only return the resolved query, nothing else:
"""
        
        try:
            prompt = PromptTemplate(
                input_variables=["query", "context"],
                template=resolution_template
            )
            
            chain = LLMChain(llm=self.llm, prompt=prompt)
            resolved_query = chain.run({
                "query": query,
                "context": context
            })
            
            return resolved_query.strip()
            
        except Exception as e:
            logging.warning(f"Failed to resolve query references: {str(e)}")
            return query
    
    def detect_references(self, query: str) -> Dict[str, Any]:
        """Detect if query contains references to previous context."""
        query_lower = query.lower()
        
        # Reference indicators
        reference_indicators = [
            "that", "it", "this", "them", "those", "these",
            "previous", "last", "recent", "above", "earlier",
            "same", "similar", "also", "too", "as well"
        ]
        
        has_references = any(indicator in query_lower for indicator in reference_indicators)
        
        return {
            "has_references": has_references,
            "indicators_found": [ind for ind in reference_indicators if ind in query_lower]
        }
