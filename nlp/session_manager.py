import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
from dataclasses import dataclass, asdict
import logging

@dataclass
class ConversationItem:
    """Represents a single conversation item."""
    query: str
    response_type: str
    sql_query: Optional[str] = None
    result_summary: Optional[str] = None
    timestamp: datetime = None
    context_used: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

class SessionManager:
    """Manages conversation session memory and context."""
    
    def __init__(self, session_id: str, max_history: int = 50):
        self.session_id = session_id
        self.max_history = max_history
        self.conversation_history: List[ConversationItem] = []
        self.session_data: Dict[str, Any] = {}
        self.session_file = f"sessions/session_{session_id}.json"
        
        # Create sessions directory if it doesn't exist
        os.makedirs("sessions", exist_ok=True)
        
        # Load existing session if available
        self._load_session()
    
    def add_interaction(self, query: str, response_type: str, 
                       sql_query: Optional[str] = None, 
                       result_summary: Optional[str] = None,
                       context_used: Optional[str] = None):
        """Add a new interaction to the conversation history."""
        item = ConversationItem(
            query=query,
            response_type=response_type,
            sql_query=sql_query,
            result_summary=result_summary,
            context_used=context_used
        )
        
        self.conversation_history.append(item)
        
        # Keep only the most recent interactions
        if len(self.conversation_history) > self.max_history:
            self.conversation_history = self.conversation_history[-self.max_history:]
        
        # Save session
        self._save_session()
        
        logging.info(f"Added interaction to session {self.session_id}")
    
    def get_conversation_history(self, limit: Optional[int] = None) -> List[ConversationItem]:
        """Get conversation history, optionally limited to recent items."""
        if limit:
            return self.conversation_history[-limit:]
        return self.conversation_history
    
    def get_last_interaction_summary(self) -> str:
        """Get a detailed summary of the last interaction."""
        history = self.get_conversation_history(limit=1)
        
        if not history:
            return "No previous interactions"
        
        last_interaction = history[-1]
        
        summary_parts = []
        summary_parts.append(f"Last Query: '{last_interaction.query}'")
        summary_parts.append(f"Type: {last_interaction.response_type}")
        
        if last_interaction.sql_query:
            # Clean up SQL formatting
            sql_clean = ' '.join(last_interaction.sql_query.split())
            if len(sql_clean) > 150:
                sql_clean = sql_clean[:150] + "..."
            summary_parts.append(f"SQL: {sql_clean}")
        
        if last_interaction.result_summary:
            summary_parts.append(f"Result: {last_interaction.result_summary}")
        
        return " | ".join(summary_parts)


    def get_recent_context(self, limit: int = 5) -> str:
        """Get recent conversation context as formatted string."""
        recent_items = self.get_conversation_history(limit)
        
        if not recent_items:
            return "No previous conversation context."
        
        context_parts = []
        for i, item in enumerate(recent_items, 1):
            context_part = f"{i}. User asked: '{item.query}'"
            if item.sql_query:
                context_part += f" (SQL: {item.sql_query[:100]}...)"
            if item.result_summary:
                context_part += f" (Result: {item.result_summary})"
            context_parts.append(context_part)
        
        return "\n".join(context_parts)
    
    def find_relevant_context(self, current_query: str, similarity_threshold: float = 0.3) -> List[ConversationItem]:
        """Find relevant previous interactions based on query similarity."""
        relevant_items = []
        current_query_lower = current_query.lower()
        
        # Simple keyword-based relevance (can be enhanced with embeddings)
        for item in self.conversation_history:
            item_query_lower = item.query.lower()
            
            # Check for common words
            current_words = set(current_query_lower.split())
            item_words = set(item_query_lower.split())
            
            # Calculate simple Jaccard similarity
            intersection = current_words.intersection(item_words)
            union = current_words.union(item_words)
            
            if union:
                similarity = len(intersection) / len(union)
                if similarity >= similarity_threshold:
                    relevant_items.append(item)
        
        return relevant_items[-3:]  # Return up to 3 most recent relevant items
    
    def detect_follow_up_patterns(self, query: str) -> Dict[str, Any]:
        """Detect if the query is a follow-up question."""
        query_lower = query.lower()
        
        # Follow-up indicators
        follow_up_indicators = [
            "that", "it", "this", "them", "those", "these",
            "previous", "last", "recent", "above", "earlier",
            "same", "similar", "also", "too", "as well"
        ]
        
        # Pronoun references
        pronoun_references = ["that", "it", "this", "them", "those", "these"]
        
        # Time references
        time_references = ["previous", "last", "recent", "earlier", "before"]
        
        # Continuation words
        continuation_words = ["also", "too", "as well", "and", "plus", "additionally"]
        
        is_follow_up = any(indicator in query_lower for indicator in follow_up_indicators)
        
        follow_up_info = {
            "is_follow_up": is_follow_up,
            "has_pronouns": any(pronoun in query_lower for pronoun in pronoun_references),
            "has_time_refs": any(time_ref in query_lower for time_ref in time_references),
            "has_continuation": any(cont in query_lower for cont in continuation_words),
            "indicators_found": [ind for ind in follow_up_indicators if ind in query_lower]
        }
        
        return follow_up_info
    
    def get_last_result_context(self) -> Optional[ConversationItem]:
        """Get the last interaction that produced results."""
        for item in reversed(self.conversation_history):
            if item.sql_query and item.response_type == "data_query":
                return item
        return None
    
    def update_session_data(self, key: str, value: Any):
        """Update session-specific data."""
        self.session_data[key] = value
        self._save_session()
    
    def get_session_data(self, key: str, default: Any = None) -> Any:
        """Get session-specific data."""
        return self.session_data.get(key, default)
    
    def clear_session(self):
        """Clear the current session."""
        self.conversation_history = []
        self.session_data = {}
        self._save_session()
        logging.info(f"Cleared session {self.session_id}")
    
    def _save_session(self):
        """Save session to file."""
        try:
            session_data = {
                "session_id": self.session_id,
                "conversation_history": [
                    {
                        **asdict(item),
                        "timestamp": item.timestamp.isoformat()
                    }
                    for item in self.conversation_history
                ],
                "session_data": self.session_data,
                "last_updated": datetime.now().isoformat()
            }
            
            with open(self.session_file, 'w') as f:
                json.dump(session_data, f, indent=2)
                
        except Exception as e:
            logging.error(f"Failed to save session: {str(e)}")
    
    def _load_session(self):
        """Load session from file."""
        try:
            if os.path.exists(self.session_file):
                with open(self.session_file, 'r') as f:
                    session_data = json.load(f)
                
                # Load conversation history
                self.conversation_history = []
                for item_data in session_data.get("conversation_history", []):
                    item_data["timestamp"] = datetime.fromisoformat(item_data["timestamp"])
                    self.conversation_history.append(ConversationItem(**item_data))
                
                # Load session data
                self.session_data = session_data.get("session_data", {})
                
                logging.info(f"Loaded session {self.session_id}")
                
        except Exception as e:
            logging.error(f"Failed to load session: {str(e)}")
    
    def cleanup_old_sessions(self, days_old: int = 7):
        """Clean up old session files."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            for filename in os.listdir("sessions"):
                if filename.startswith("session_") and filename.endswith(".json"):
                    file_path = os.path.join("sessions", filename)
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    if file_time < cutoff_date:
                        os.remove(file_path)
                        logging.info(f"Cleaned up old session file: {filename}")
                        
        except Exception as e:
            logging.error(f"Failed to cleanup old sessions: {str(e)}")
