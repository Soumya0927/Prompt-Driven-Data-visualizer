import streamlit as st
import pandas as pd
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import traceback
import tempfile
import os
import uuid

# === Internal Modules === #
from utils.mysql_connector import MySQLConnector
from utils.postgres_connector import PostgreSQLConnector
from utils.sqlite_connector import SQLiteConnector
from nlp.context_aware_processor import ContextAwareQueryProcessor
from nlp.session_manager import SessionManager
from visualization.chart_generator import ChartGenerator
from utils.error_handler import ErrorHandler
from utils.logger import Logger

# === App Configuration === #
st.set_page_config(
    page_title="DataViz ChatBot - Context Aware",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === Custom CSS === #
st.markdown("""
<style>
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        display: flex;
        align-items: flex-start;
        overflow-wrap: anywhere;
        word-wrap: break-word;
        width: 100%;
    }
    .user-message {
        background-color: #e3f2fd;
        border-left: 4px solid #2196f3;
    }
    .bot-message {
        background-color: #f1f8e9;
        border-left: 4px solid #4caf50;
    }
    .context-message {
        background-color: #fff3e0;
        border-left: 4px solid #ff9800;
        font-size: 0.9em;
    }
    .error-message {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
    }
    .session-info {
        background-color: #f5f5f5;
        padding: 0.5rem;
        border-radius: 0.3rem;
        margin: 0.5rem 0;
        font-size: 0.8em;
    }
</style>
""", unsafe_allow_html=True)

# === Initialize Session State === #
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'db_connector' not in st.session_state:
    st.session_state.db_connector = None
if 'query_processor' not in st.session_state:
    st.session_state.query_processor = None
if 'session_manager' not in st.session_state:
    st.session_state.session_manager = None
if 'db_type' not in st.session_state:
    st.session_state.db_type = None

# === Sidebar Configuration === #
st.sidebar.title("🔧 Database Configuration")

# Database selection
db_type = st.sidebar.selectbox("Select Database Type", ["SQLite", "PostgreSQL", "MySQL"])

# === SQLite Configuration === #
if db_type == "SQLite":
    st.sidebar.subheader("📁 SQLite Options")
    
    sqlite_option = st.sidebar.radio(
        "Choose SQLite source:",
        ["Upload CSV Files", "Upload Database File", "Local Database Path"]
    )
    
    if sqlite_option == "Upload CSV Files":
        st.sidebar.markdown("### 📊 Upload CSV Files")
        uploaded_csvs = st.sidebar.file_uploader(
            "Upload CSV files",
            type=['csv'],
            accept_multiple_files=True,
            help="Upload one or more CSV files to create tables"
        )
        
        if uploaded_csvs and st.sidebar.button("🔄 Create Database from CSVs"):
            try:
                connector = SQLiteConnector()
                success_count = 0
                for csv_file in uploaded_csvs:
                    table_name = os.path.splitext(csv_file.name)[0]
                    if connector.create_table_from_csv(csv_file, table_name):
                        success_count += 1
                
                if success_count > 0:
                    st.session_state.db_connector = connector
                    st.session_state.session_manager = SessionManager(st.session_state.session_id)
                    st.session_state.query_processor = ContextAwareQueryProcessor(
                        connector, "sqlite", st.session_state.session_manager
                    )
                    st.session_state.db_type = "sqlite"
                    st.sidebar.success(f"✅ Created {success_count} tables from CSV files!")
                else:
                    st.sidebar.error("❌ Failed to create tables from CSV files")
                    
            except Exception as e:
                st.sidebar.error(f"❌ Error: {str(e)}")
    
    elif sqlite_option == "Upload Database File":
        st.sidebar.markdown("### 🗄️ Upload Database File")
        uploaded_db = st.sidebar.file_uploader(
            "Upload SQLite database file",
            type=['db', 'sqlite', 'sqlite3'],
            help="Upload an existing SQLite database file"
        )
        
        if uploaded_db and st.sidebar.button("🔗 Connect to Uploaded Database"):
            try:
                connector = SQLiteConnector(uploaded_file=uploaded_db)
                if connector.test_connection():
                    st.session_state.db_connector = connector
                    st.session_state.session_manager = SessionManager(st.session_state.session_id)
                    st.session_state.query_processor = ContextAwareQueryProcessor(
                        connector, "sqlite", st.session_state.session_manager
                    )
                    st.session_state.db_type = "sqlite"
                    st.sidebar.success("✅ Connected to uploaded database!")
                else:
                    st.sidebar.error("❌ Connection failed!")
            except Exception as e:
                st.sidebar.error(f"❌ Error: {str(e)}")
    
    else:  # Local Database Path
        st.sidebar.markdown("### 📂 Local Database Path")
        db_path = st.sidebar.text_input(
            "Database file path",
            value="",
            help="Enter the full path to your SQLite database file"
        )
        
        if db_path and st.sidebar.button("🔗 Connect to Local Database"):
            try:
                if os.path.exists(db_path):
                    connector = SQLiteConnector(database_path=db_path)
                    if connector.test_connection():
                        st.session_state.db_connector = connector
                        st.session_state.session_manager = SessionManager(st.session_state.session_id)
                        st.session_state.query_processor = ContextAwareQueryProcessor(
                            connector, "sqlite", st.session_state.session_manager
                        )
                        st.session_state.db_type = "sqlite"
                        st.sidebar.success("✅ Connected to local database!")
                    else:
                        st.sidebar.error("❌ Connection failed!")
                else:
                    st.sidebar.error("❌ Database file not found!")
            except Exception as e:
                st.sidebar.error(f"❌ Error: {str(e)}")

# === PostgreSQL Configuration === #
elif db_type == "PostgreSQL":
    with st.sidebar.expander("Database Connection", expanded=True):
        host = st.text_input("Host", value="localhost")
        port = st.number_input("Port", value=5432)
        database = st.text_input("Database", value="mydb")
        username = st.text_input("Username", value="myuser")
        password = st.text_input("Password", value="mypassword", type="password")
    
    if st.sidebar.button("🔗 Connect to PostgreSQL"):
        try:
            connector = PostgreSQLConnector(host, port, database, username, password)
            if connector.test_connection():
                st.session_state.db_connector = connector
                st.session_state.session_manager = SessionManager(st.session_state.session_id)
                st.session_state.query_processor = ContextAwareQueryProcessor(
                    connector, "postgresql", st.session_state.session_manager
                )
                st.session_state.db_type = "postgresql"
                st.sidebar.success("✅ Connected successfully!")
            else:
                st.sidebar.error("❌ Connection failed!")
        except Exception as e:
            st.sidebar.error(f"❌ Error: {str(e)}")

# === MySQL Configuration === #
else:  # MySQL
    with st.sidebar.expander("Database Connection", expanded=True):
        host = st.text_input("Host", value="localhost")
        port = st.number_input("Port", value=3306)
        database = st.text_input("Database", value="mydb")
        username = st.text_input("Username", value="myuser")
        password = st.text_input("Password", value="mypassword", type="password")
    
    if st.sidebar.button("🔗 Connect to MySQL"):
        try:
            connector = MySQLConnector(host, port, database, username, password)
            if connector.test_connection():
                st.session_state.db_connector = connector
                st.session_state.session_manager = SessionManager(st.session_state.session_id)
                st.session_state.query_processor = ContextAwareQueryProcessor(
                    connector, "mysql", st.session_state.session_manager
                )
                st.session_state.db_type = "mysql"
                st.sidebar.success("✅ Connected successfully!")
            else:
                st.sidebar.error("❌ Connection failed!")
        except Exception as e:
            st.sidebar.error(f"❌ Error: {str(e)}")

# === Main App === #
st.title("🤖 DataViz ChatBot - Context Aware")
st.markdown("### Ask me anything about your data! I remember our conversation.")

# Show connection status and session info
if st.session_state.db_connector:
    col1, col2 = st.columns([2, 1])
    with col1:
        st.success(f"🔗 Connected to {st.session_state.db_type.upper()} database")
    with col2:
        if st.session_state.session_manager:
            context_count = len(st.session_state.session_manager.get_conversation_history())
            st.info(f"💭 Context: {context_count} interactions")
else:
    st.info("👆 Please configure and connect to a database using the sidebar")

# === Chat Interface === #
chat_container = st.container()

with chat_container:
    # Display chat history
    for message in st.session_state.messages:
        if message["role"] == "user":
            st.markdown(f"""
            <div class="chat-message user-message">
                <strong>You:</strong> {message["content"]}
            </div>
            """, unsafe_allow_html=True)
            
        elif message["role"] == "assistant":
            st.markdown('<div class="chat-message bot-message"><strong>Bot:</strong></div>', unsafe_allow_html=True)
            st.markdown(message["content"])
            
            # # Show context information if available
            # if "context_used" in message and message["context_used"]:
            #     # Format context for better display
            #     context_display = message["context_used"]
            #     if len(context_display) > 200:
            #         context_display = context_display[:200] + "..."
                
            #     st.markdown(f"""
            #     <div class="context-message">
            #         <strong>Context Used:</strong> {context_display}
            #     </div>
            #     """, unsafe_allow_html=True)
            
            # Display SQL query if available
            if "sql_query" in message:
                st.code(message["sql_query"], language="sql")
                
            # Display dataframe if available
            if "dataframe" in message:
                st.dataframe(message["dataframe"])
            
            # Display chart if available
            if "chart" in message:
                try:
                    st.plotly_chart(message["chart"], use_container_width=True)
                except Exception as e:
                    # Silently handle chart errors - don't display anything
                    pass            
        elif message["role"] == "error":
            st.markdown(f"""
            <div class="chat-message error-message">
                <strong>Error:</strong> {message["content"]}
            </div>
            """, unsafe_allow_html=True)

# === User Input === #
user_input = st.chat_input("Ask me about your data...")

if user_input and st.session_state.db_connector:
    # Add user message to chat
    st.session_state.messages.append({
        "role": "user", 
        "content": user_input,
        "timestamp": datetime.now()
    })
    
    try:
        # Process the query with context awareness
        with st.spinner("🤔 Thinking with context..."):
            response = st.session_state.query_processor.process_query(user_input)
            
            if response["type"] == "data_query":
                # Execute SQL and get results
                df = st.session_state.db_connector.execute_query(response["sql_query"])
                
                # Generate appropriate visualization
                chart_generator = ChartGenerator()
                chart = chart_generator.generate_chart(df, user_input)
                
                # Add assistant response
                assistant_message = {
                    "role": "assistant",
                    "content": response["explanation"],
                    "sql_query": response["sql_query"],
                    "dataframe": df,
                    "chart": chart,
                    "context_used": response.get("context_used", ""),
                    "timestamp": datetime.now()
                }
                
                st.session_state.messages.append(assistant_message)
                
            elif response["type"] == "schema_query":
                # Return schema information
                schema_info = st.session_state.db_connector.get_schema_info()
                
                assistant_message = {
                    "role": "assistant",
                    "content": f"Here's your database schema:\n\n{schema_info}",
                    "context_used": response.get("context_used", ""),
                    "timestamp": datetime.now()
                }
                
                st.session_state.messages.append(assistant_message)
                
            elif response["type"] == "general_response":
                # General AI response
                assistant_message = {
                    "role": "assistant",
                    "content": response["content"],
                    "context_used": response.get("context_used", ""),
                    "timestamp": datetime.now()
                }
                
                st.session_state.messages.append(assistant_message)
                
            else:
                raise ValueError("Unknown response type")
        
        Logger.log_info(f"Processed query with context: {user_input}")
        
    except Exception as e:
        error_message = ErrorHandler.handle_error(e)
        st.session_state.messages.append({
            "role": "error",
            "content": error_message,
            "timestamp": datetime.now()
        })
        Logger.log_error(f"Query processing failed: {str(e)}")
    
    # Rerun to display new messages
    st.rerun()

elif user_input and not st.session_state.db_connector:
    st.error("Please connect to a database first!")

# === Sidebar Info === #
with st.sidebar:
    st.markdown("---")
    st.markdown("### 🧠 Context Features")
    st.markdown("""
    - 💭 **Session Memory**: Remembers conversation
    - 🔄 **Follow-up Questions**: Understands "it", "that", "previous"
    - 📊 **Iterative Exploration**: Builds on previous queries
    - 🎯 **Smart Context**: Uses relevant past interactions
    """)
    
    if st.session_state.session_manager:
        st.markdown("---")
        st.markdown("### 📈 Session Stats")
        history = st.session_state.session_manager.get_conversation_history()
        st.markdown(f"**Total Interactions:** {len(history)}")
        st.markdown(f"**Session ID:** {st.session_state.session_id[:8]}...")
        
        if st.button("🔍 View Context"):
            with st.expander("Conversation Context", expanded=True):
                for i, item in enumerate(history[-5:]):  # Show last 5 interactions
                    st.markdown(f"**{i+1}.** {item['query'][:50]}...")
    
    if st.session_state.db_connector:
        st.markdown("---")
        st.markdown("### 📊 Database Info")
        try:
            tables = st.session_state.db_connector.get_tables()
            st.markdown(f"**Tables:** {len(tables)}")
            with st.expander("View Tables"):
                for table in tables:
                    st.markdown(f"- {table}")
        except Exception as e:
            st.error(f"Error fetching tables: {str(e)}")
    
    # Clear chat button
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        if st.session_state.session_manager:
            st.session_state.session_manager.clear_session()
        st.rerun()

# === Footer === #
st.markdown("---")
st.markdown("### 💡 Try Context-Aware Queries:")
st.markdown("""
- "Show me sales data" → "What about last month?" → "Create a chart for that"
- "Find top customers" → "Show their orders" → "Which products do they buy most?"
- "Analyze revenue trends" → "Break it down by region" → "Focus on the highest performing one"
""")
def display_context_info(session_manager):
    """Display context information in sidebar."""
    if not session_manager:
        return
    
    with st.sidebar:
        st.markdown("---")
        st.markdown("### 💭 Current Context")
        
        # Get last interaction summary
        last_summary = session_manager.get_last_interaction_summary()
        if last_summary != "No previous interactions":
            with st.expander("Last Interaction", expanded=False):
                st.markdown(last_summary)
        
        # Show context statistics
        history_count = len(session_manager.get_conversation_history())
        st.markdown(f"**Total Interactions:** {history_count}")
        
        # Show recent queries
        if history_count > 0:
            recent_queries = session_manager.get_conversation_history(limit=3)
            with st.expander("Recent Queries", expanded=False):
                for i, item in enumerate(recent_queries, 1):
                    st.markdown(f"**{i}.** {item.query}")
                    if item.response_type == "data_query":
                        st.markdown(f"   ↳ *{item.response_type}*")

# Add this to your main app:
display_context_info(st.session_state.session_manager)

