import logging
import os
import sys
import datetime

import streamlit as st
from databricks.sdk import WorkspaceClient
import json
from view_groups import groups

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Configure logging - output to both console and file
logging.basicConfig(
    level=logging.INFO,  # Set to INFO level to ensure all informational logs are visible
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),  # Output to console
        logging.FileHandler("logs/app.log")  # Output to file
    ]
)

# Create a dedicated logger for table access
table_logger = logging.getLogger("table-access")
table_logger.setLevel(logging.INFO)  # Ensure level is INFO

# Configure app
st.set_page_config(
    page_title="Databricks App",
    page_icon=":factory:",
    layout="wide",
)

def get_user_info_from_headers():
    """
    Get user information from Streamlit context
    
    Returns:
        dict: Dictionary containing username, email and IP address
    """
    try:
        headers = st.context.headers
        user_info = {
            "username": headers.get("X-Forwarded-Preferred-Username", "unknown"),
            "email": headers.get("X-Forwarded-Email", "unknown"),
            "ip_address": headers.get("X-Real-Ip", "unknown")
        }
        # Print user information directly to console for debugging
        print(f"USER INFO: {json.dumps(user_info)}")
        return user_info
    except Exception as e:
        print(f"ERROR getting user info: {str(e)}")
        return {
            "username": "unknown",
            "email": "unknown",
            "ip_address": "unknown"
        }

def log_table_operation(operation, table_name, user_info=None):
    """
    Log a table operation with user information.

    Args:
        operation: The operation performed (e.g., 'read', 'read_failed')
        table_name: The full name of the table (catalog.schema.table)
        user_info: Dictionary containing user information and operation details
    """
    # Ensure user_info is at least an empty dict if None
    if user_info is None:
        try:
            user_info = get_user_info_from_headers()
        except Exception as e:
            # Fallback to WorkspaceClient user information
            try:
                w = WorkspaceClient()
                user_name = w.current_user.me().user_name
                user_info = {
                    "username": user_name,
                    "email": user_name,
                    "ip_address": "unknown"
                }
            except:
                # If all else fails, use defaults
                user_info = {
                    "username": "unknown",
                    "email": "unknown",
                    "ip_address": "unknown"
                }
            print(f"Unable to get user info from headers, using fallback: {str(e)}")
    
    # Create the log entry with required fields
    log_data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "operation": operation,
        "table_name": table_name,
        "username": user_info.get("username", "unknown") if isinstance(user_info, dict) else "unknown",
        "email": user_info.get("email", "unknown") if isinstance(user_info, dict) else "unknown",
        "ip_address": user_info.get("ip_address", "unknown") if isinstance(user_info, dict) else "unknown",
        "status": user_info.get("status", "unknown") if isinstance(user_info, dict) else "unknown"
    }
    
    # Add additional fields that may be useful
    if isinstance(user_info, dict):
        for field in ["target_table", "warehouse", "rows_count", "query_time", "error"]:
            if field in user_info:
                log_data[field] = user_info[field]
    
    # Determine if this is a success or failure log
    is_failure = operation == "read_failed" or (isinstance(user_info, dict) and user_info.get("status") == "failed")
    
    # Create a single log message
    log_message = f"TABLE_ACCESS: {json.dumps(log_data)}"
    
    # Log at appropriate level
    if is_failure:
        # Log failures with warning level
        print(f"WARNING: {log_message}")
        table_logger.warning(log_message)
        logging.getLogger("databricks-app").warning(log_message)
    else:
        # Log success with info level
        print(log_message)
        table_logger.info(log_message)
        logging.getLogger("databricks-app").info(log_message)
    
    # Optional: Display in UI for debugging
    if st.session_state.get("debug_mode", False):
        if is_failure:
            st.sidebar.error(f"{operation} - {table_name} - FAILED")
        else:
            st.sidebar.success(f"{operation} - {table_name} - SUCCESS")

# Add debug switch to session state
if "debug_mode" not in st.session_state:
    st.session_state["debug_mode"] = False

# Store the log function in session state so it can be accessed from other files
st.session_state["log_table_operation"] = log_table_operation
st.session_state["get_user_info"] = get_user_info_from_headers

# Log application start
logging.getLogger("databricks-app").info("Databricks Cookbook application started")
print("Databricks Cookbook application started - STDOUT")

# Optional debug mode toggle button (in sidebar)
with st.sidebar:
    if st.checkbox("Debug Mode", value=st.session_state["debug_mode"]):
        st.session_state["debug_mode"] = True
    else:
        st.session_state["debug_mode"] = False

st.logo("assets/logo.svg")
st.title("📖 Databricks Apps Cookbook 🍳")

pages = {
    group.get("title", ""): [
        st.Page(
            view.get("page"),
            title=view.get("label"),
            icon=view.get("icon"),
        )
        for view in group["views"]
    ]
    for group in groups
}

pg = st.navigation(pages)
pg.run()
