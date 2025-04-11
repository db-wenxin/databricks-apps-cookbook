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
        operation: The operation performed (e.g., 'read', 'write', 'read_failed')
        table_name: The full name of the table (catalog.schema.table)
        user_info: Dictionary containing user information and other details
    """
    if user_info is None:
        # Get user information from Streamlit context
        try:
            user_info = get_user_info_from_headers()
        except Exception as e:
            # Fallback to WorkspaceClient user information
            w = WorkspaceClient()
            user_name = w.current_user.me().user_name
            user_info = {
                "username": user_name,
                "email": user_name,
                "ip_address": "unknown"
            }
            print(f"Unable to get user info from headers, using WorkspaceClient as fallback: {str(e)}")
    
    # Create base log data
    log_data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "operation": operation,
        "table_name": table_name,
        "app_id": os.environ.get("DATABRICKS_APPS_APP_ID", "local-dev")
    }
    
    # Add user information
    for key in ["username", "email", "ip_address"]:
        if key in user_info:
            log_data[key] = user_info.get(key, "unknown")
    
    # Add all additional fields from user_info
    for key, value in user_info.items():
        if key not in ["username", "email", "ip_address"]:
            log_data[key] = value
    
    # Format log message differently based on operation type
    if operation == "read_failed" or "status" in user_info and user_info["status"] == "failed":
        error_msg = user_info.get("error", "Unknown error")
        error_type = user_info.get("error_type", "UnknownError")
        log_message = f"TABLE_ACCESS_FAILED: {json.dumps(log_data)} - Error: {error_type}: {error_msg}"
        
        # Log failures with warning level
        print(f"WARNING: {log_message}")
        table_logger.warning(log_message)
        logging.getLogger("databricks-app").warning(log_message)
    else:
        log_message = f"TABLE_ACCESS: {json.dumps(log_data)}"
        
        # Log success with info level
        print(log_message)
        table_logger.info(log_message)
        logging.getLogger("databricks-app").info(log_message)
    
    # Optional: Display log information in Streamlit UI (for debugging only)
    if st.session_state.get("debug_mode", False):
        if operation == "read_failed" or "status" in user_info and user_info["status"] == "failed":
            st.sidebar.error(f"Failed: {operation} on {table_name} - {user_info.get('error', 'Unknown error')}")
        else:
            st.sidebar.info(f"Logged: {operation} on {table_name} by {user_info.get('username', 'unknown')}")
            if "rows_count" in user_info:
                st.sidebar.success(f"Read {user_info['rows_count']} rows")

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
