import os
from typing import Optional, List
from llama_index.core import Document, VectorStoreIndex, Settings
from llama_index.embeddings.openai import OpenAIEmbedding
from langchain_groq import ChatGroq
from llama_index.core.node_parser import SentenceSplitter
import logging

class LlamaIndexManager:
    """Manages LlamaIndex components for semantic search."""
    
    def __init__(self, groq_api_key: str, openai_api_key: Optional[str] = None):
        self.groq_api_key = groq_api_key
        self.openai_api_key = openai_api_key
        self.schema_index = None
        self.context_index = None
        self._setup_llama_index()
    
    def _setup_llama_index(self):
        """Setup LlamaIndex components."""
        try:
            # Configure LlamaIndex settings
            Settings.llm = ChatGroq(
                model="llama3-70b-8192",
                api_key=self.groq_api_key
            )
            
            # Use OpenAI embeddings if available, otherwise use default
            if self.openai_api_key:
                Settings.embed_model = OpenAIEmbedding(
                    api_key=self.openai_api_key
                )
            
            logging.info("LlamaIndex setup completed")
            
        except Exception as e:
            logging.error(f"Failed to setup LlamaIndex: {str(e)}")
    
    def build_schema_index(self, db_connector):
        """Build LlamaIndex for database schema."""
        try:
            schema_info = db_connector.get_schema_info()
            tables = db_connector.get_tables()
            
            documents = []
            
            # Create document for overall schema
            schema_doc = Document(
                text=f"Database Schema Information:\n{schema_info}",
                metadata={"type": "schema", "database_type": db_connector.db_type}
            )
            documents.append(schema_doc)
            
            # Create documents for each table
            for table in tables:
                try:
                    table_info = db_connector.get_column_info(table)
                    sample_data = db_connector.get_table_sample(table, 3)
                    
                    table_text = f"Table: {table}\n"
                    table_text += f"Columns: {[col['column_name'] for col in table_info]}\n"
                    table_text += f"Sample data:\n{sample_data.to_string()}\n"
                    
                    table_doc = Document(
                        text=table_text,
                        metadata={"type": "table", "table_name": table}
                    )
                    documents.append(table_doc)
                    
                except Exception as e:
                    logging.warning(f"Failed to process table {table}: {str(e)}")
            
            # Build index
            if documents:
                self.schema_index = VectorStoreIndex.from_documents(documents)
                logging.info(f"Built schema index with {len(documents)} documents")
            
        except Exception as e:
            logging.error(f"Failed to build schema index: {str(e)}")
    
    def build_context_index(self, conversation_history):
        """Build LlamaIndex for conversation context."""
        try:
            if not conversation_history:
                return
            
            documents = []
            
            for i, item in enumerate(conversation_history):
                context_text = f"Interaction {i+1}:\n"
                context_text += f"User Query: {item.query}\n"
                context_text += f"Response Type: {item.response_type}\n"
                
                if item.sql_query:
                    context_text += f"SQL Query: {item.sql_query}\n"
                
                if item.result_summary:
                    context_text += f"Result Summary: {item.result_summary}\n"
                
                context_doc = Document(
                    text=context_text,
                    metadata={
                        "type": "conversation",
                        "interaction_id": i,
                        "timestamp": item.timestamp.isoformat()
                    }
                )
                documents.append(context_doc)
            
            # Build index
            if documents:
                self.context_index = VectorStoreIndex.from_documents(documents)
                logging.info(f"Built context index with {len(documents)} documents")
            
        except Exception as e:
            logging.error(f"Failed to build context index: {str(e)}")
    
    def query_schema(self, query: str) -> str:
        """Query the schema index."""
        if not self.schema_index:
            return ""
        
        try:
            query_engine = self.schema_index.as_query_engine()
            response = query_engine.query(query)
            return str(response)
        except Exception as e:
            logging.error(f"Failed to query schema index: {str(e)}")
            return ""
    
    def query_context(self, query: str) -> str:
        """Query the context index."""
        if not self.context_index:
            return ""
        
        try:
            query_engine = self.context_index.as_query_engine()
            response = query_engine.query(query)
            return str(response)
        except Exception as e:
            logging.error(f"Failed to query context index: {str(e)}")
            return ""
