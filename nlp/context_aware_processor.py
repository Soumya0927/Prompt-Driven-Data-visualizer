import os
from typing import Dict, Any
from langchain_groq import ChatGroq
from dotenv import load_dotenv
import logging

from .session_manager import SessionManager
from .database_templates import DatabaseTemplates
from .context_manager import ContextManager
from .query_classifier import QueryClassifier
from .sql_generator import SQLGenerator
from .reference_resolver import ReferenceResolver
from .llama_index_manager import LlamaIndexManager

# Load environment variables
load_dotenv()

class ContextAwareQueryProcessor:
    """Main coordinator for context-aware query processing."""
    
    def __init__(self, db_connector, db_type: str, session_manager: SessionManager):
        self.db_connector = db_connector
        self.db_type = db_type
        self.session_manager = session_manager
        
        # Initialize LLM
        self.llm = ChatGroq(
            model_name="llama3-70b-8192",
            groq_api_key=os.getenv("GROQ_API_KEY"),
            temperature=0.1
        )
        
        # Initialize components
        self.context_manager = ContextManager(session_manager)
        self.query_classifier = QueryClassifier()
        self.sql_generator = SQLGenerator(self.llm, db_connector)
        self.reference_resolver = ReferenceResolver(self.llm)
        self.llama_index_manager = LlamaIndexManager(
            groq_api_key=os.getenv("GROQ_API_KEY"),
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Get database-specific template
        self.sql_template = DatabaseTemplates.get_template_for_database(db_type)
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process user query with intelligent context awareness."""
        try:
            # Get relevant context
            follow_up_info = self.session_manager.detect_follow_up_patterns(user_query)
            relevant_context = self.context_manager.get_relevant_context(user_query, follow_up_info)
            
            # Resolve references
            resolved_query = self.reference_resolver.resolve_query_references(user_query, relevant_context)
            
            # Classify query type
            query_type = self.query_classifier.classify_query(resolved_query)
            
            # Process based on type
            if query_type == "schema_request":
                response = self._process_schema_query(resolved_query, relevant_context)
            elif query_type == "table_list":
                response = self._process_table_list_query(resolved_query, relevant_context)
            elif query_type == "data_query":
                response = self._process_data_query(resolved_query, relevant_context)
            elif query_type == "general":
                response = self._process_general_query(resolved_query, relevant_context)
            else:
                response = {
                    "type": "general_response",
                    "content": "I'm not sure how to help with that. Try asking about your data or database schema.",
                    "context_used": ""
                }
            
            # Add interaction to session
            self.session_manager.add_interaction(
                query=user_query,
                response_type=response["type"],
                sql_query=response.get("sql_query"),
                result_summary=response.get("result_summary"),
                context_used=response.get("context_used", "")
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Query processing failed: {str(e)}")
            raise
    
    def _process_data_query(self, query: str, context: str) -> Dict[str, Any]:
        """Process data-related queries."""
        try:
            # Generate SQL query
            sql_query = self.sql_generator.generate_sql(query, context, self.sql_template)
            
            # Determine context usage
            context_usage = self.context_manager.determine_context_usage(
                query, context, sql_query, self.llm
            )
            
            return {
                "type": "data_query",
                "sql_query": sql_query,
                "explanation": f"I'll execute this query to answer your question: '{query}'",
                "context_used": context_usage,
                "result_summary": "Query executed successfully"
            }
            
        except Exception as e:
            logging.error(f"Data query processing failed: {str(e)}")
            raise ValueError(f"Failed to process data query: {str(e)}")
    
    def _process_schema_query(self, query: str, context: str) -> Dict[str, Any]:
        """Process schema-related queries."""
        return {
            "type": "schema_query",
            "content": "Here's your database schema information.",
            "context_used": context[:100] + "..." if len(context) > 100 else context
        }
    
    def _process_table_list_query(self, query: str, context: str) -> Dict[str, Any]:
        """Process table list queries."""
        return {
            "type": "schema_query",
            "content": "Here are all the tables in your database.",
            "context_used": context[:100] + "..." if len(context) > 100 else context
        }
    
    def _process_general_query(self, query: str, context: str) -> Dict[str, Any]:
        """Process general queries."""
        try:
            from langchain.chains import LLMChain
            from langchain_core.prompts import PromptTemplate
            
            general_template = """
You are a helpful data analysis assistant. The user has asked a question about data analysis or databases.
Use the conversation context to provide a more relevant and helpful response.

Conversation Context:
{context}

User Question: {question}

Response:
"""
            
            prompt = PromptTemplate(
                input_variables=["question", "context"],
                template=general_template
            )
            
            chain = LLMChain(llm=self.llm, prompt=prompt)
            response = chain.run({"question": query, "context": context})
            
            return {
                "type": "general_response",
                "content": response,
                "context_used": context[:100] + "..." if len(context) > 100 else context
            }
            
        except Exception as e:
            logging.error(f"General query processing failed: {str(e)}")
            return {
                "type": "general_response",
                "content": "I'm sorry, I couldn't process your request. Please try asking about your data or database schema.",
                "context_used": ""
            }
