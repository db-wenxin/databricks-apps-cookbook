import logging
import os
import sys
import datetime

import streamlit as st
from databricks.sdk import WorkspaceClient
import json
from view_groups import groups

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("databricks-app")

# Configure the app
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
    headers = st.context.headers
    return {
        "username": headers.get("X-Forwarded-Preferred-Username", "unknown"),
        "email": headers.get("X-Forwarded-Email", "unknown"),
        "ip_address": headers.get("X-Real-Ip", "unknown")
    }

def log_table_operation(operation, table_name, user_info=None):
    """
    Log a table operation with user information.

    Args:
        operation: The operation performed (e.g., 'read', 'write')
        table_name: The full name of the table (catalog.schema.table)
        user_info: Dictionary containing user information from HTTP headers
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
            logger.warning(f"Unable to get user info from headers, using WorkspaceClient as fallback: {str(e)}")
    
    log_data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "operation": operation,
        "table_name": table_name,
        "username": user_info["username"],
        "email": user_info["email"],
        "ip_address": user_info["ip_address"],
        "app_id": os.environ.get("DATABRICKS_APPS_APP_ID", "local-dev")
    }
    
    logger.info(f"TABLE_ACCESS: {json.dumps(log_data)}")

# Example: How to use the logging function in views
# 
# ```python
# # When using in a view function
# headers = st.context.headers
# user_info = {
#     "username": headers.get("X-Forwarded-Preferred-Username", "unknown"),
#     "email": headers.get("X-Forwarded-Email", "unknown"),
#     "ip_address": headers.get("X-Real-Ip", "unknown")
# }
# 
# # Log access using the retrieved user information
# st.session_state["log_table_operation"]("read", "catalog.schema.table_name", user_info)
# ```

# Store the log function in session state so it can be accessed from other files
st.session_state["log_table_operation"] = log_table_operation
st.session_state["get_user_info"] = get_user_info_from_headers

# Log application start
logger.info("Databricks Cookbook application started")

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
