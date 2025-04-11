import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
import datetime
import json

st.header(body="Tables", divider=True)
st.subheader("Read a table")
st.write(
    "This recipe reads a Unity Catalog table using the [Databricks SQL Connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html)."
)

cfg = Config()

w = WorkspaceClient()

warehouses = w.warehouses.list()

warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}

catalogs = w.catalogs.list()


@st.cache_resource
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def read_table(table_name, conn):
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


def get_schema_names(catalog_name):
    schemas = w.schemas.list(catalog_name=catalog_name)
    return [schema.name for schema in schemas]


def get_table_names(catalog_name, schema_name):
    tables = w.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    return [table.name for table in tables]


tab_a, tab_b, tab_c = st.tabs(["**Try it**", "**Code snippet**", "**Requirements**"])

with tab_a:
    http_path_input = st.selectbox(
        "Select a SQL warehouse:", [""] + list(warehouse_paths.keys())
    )

    catalog_name = st.selectbox(
        "Select a catalog:", [""] + [catalog.name for catalog in catalogs]
    )

    if catalog_name and catalog_name != "":
        schema_names = get_schema_names(catalog_name)
        schema_name = st.selectbox("Select a schema:", [""] + schema_names)

    if catalog_name and catalog_name != "" and schema_name and schema_name != "":
        table_names = get_table_names(catalog_name, schema_name)
        table_name = st.selectbox("Select a table:", [""] + table_names)

        if http_path_input and table_name and table_name != "":
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
            
            if st.button("Read Data"):
                with st.spinner(f"Reading data from {full_table_name}..."):
                    # Get user information for logging
                    user_info = None
                    if "get_user_info" in st.session_state:
                        try:
                            user_info = st.session_state["get_user_info"]()
                        except Exception as e:
                            print(f"Error getting user info: {str(e)}")
                            user_info = {
                                "username": "unknown",
                                "email": "unknown",
                                "ip_address": "unknown"
                            }
                    
                    # Try to read the table
                    try:
                        http_path = warehouse_paths[http_path_input]
                        conn = get_connection(http_path)
                        
                        # Execute the query
                        start_time = datetime.datetime.now()
                        df = read_table(full_table_name, conn)
                        
                        # Create log entry for successful read
                        if "log_table_operation" in st.session_state:
                            log_data = user_info.copy() if user_info else {}
                            log_data.update({
                                "target_table": full_table_name,
                                "warehouse": http_path_input,
                                "status": "success",
                                "rows_count": len(df),
                                "query_time": start_time.isoformat()
                            })
                            st.session_state["log_table_operation"]("read", full_table_name, log_data)
                        
                        # Display success and data
                        st.success(f"Successfully read {len(df)} rows")
                        st.dataframe(df)
                    
                    except Exception as e:
                        # Create log entry for failed read
                        if "log_table_operation" in st.session_state:
                            log_data = user_info.copy() if user_info else {}
                            log_data.update({
                                "target_table": full_table_name,
                                "warehouse": http_path_input,
                                "status": "failed",
                                "error": str(e),
                                "query_time": datetime.datetime.now().isoformat()
                            })
                            st.session_state["log_table_operation"]("read_failed", full_table_name, log_data)
                        
                        # Display error to user
                        error_msg = str(e)
                        if "403" in error_msg and "FORBIDDEN" in error_msg:
                            st.error(f"Access denied: You don't have permission to read table {full_table_name}. Please check your credentials or request access.", icon="🔒")
                        else:
                            st.error(f"Failed to read table: {error_msg}", icon="❌")


with tab_b:
    st.code(
        """
        import streamlit as st
        from databricks import sql
        from databricks.sdk.core import Config
        from databricks.sdk import WorkspaceClient


        cfg = Config()  # Set the DATABRICKS_HOST environment variable when running locally
        
        # Initialize workspace client
        w = WorkspaceClient()
        
        # Get warehouses and catalogs
        warehouses = w.warehouses.list()
        warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}
        catalogs = w.catalogs.list()


        @st.cache_resource # connection is cached
        def get_connection(http_path):
            return sql.connect(
                server_hostname=cfg.host,
                http_path=http_path,
                credentials_provider=lambda: cfg.authenticate,
            )

        def read_table(table_name, conn):
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
                
        # Instead of text inputs, use dropdowns to select resources
        http_path_input = st.selectbox(
            "Select a SQL warehouse:", [""] + list(warehouse_paths.keys())
        )

        catalog_name = st.selectbox(
            "Select a catalog:", [""] + [catalog.name for catalog in catalogs]
        )

        if catalog_name and catalog_name != "":
            schemas = w.schemas.list(catalog_name=catalog_name)
            schema_name = st.selectbox("Select a schema:", [""] + [schema.name for schema in schemas])

        if catalog_name and catalog_name != "" and schema_name and schema_name != "":
            tables = w.tables.list(catalog_name=catalog_name, schema_name=schema_name)
            table_name = st.selectbox("Select a table:", [""] + [table.name for table in tables])

            if http_path_input and table_name and table_name != "":
                full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
                
                # Add Read button - data is only fetched when the button is clicked
                if st.button("Read Data"):
                    with st.spinner(f"Reading data from {full_table_name}..."):
                        # Get user information for logging
                        user_info = None
                        if "get_user_info" in st.session_state:
                            try:
                                user_info = st.session_state["get_user_info"]()
                            except Exception as e:
                                print(f"Error getting user info: {str(e)}")
                                user_info = {
                                    "username": "unknown",
                                    "email": "unknown",
                                    "ip_address": "unknown"
                                }
                        
                        # Try to read the table
                        try:
                            http_path = warehouse_paths[http_path_input]
                            conn = get_connection(http_path)
                            
                            # Execute the query
                            start_time = datetime.datetime.now()
                            df = read_table(full_table_name, conn)
                            
                            # Create log entry for successful read
                            if "log_table_operation" in st.session_state:
                                log_data = user_info.copy() if user_info else {}
                                log_data.update({
                                    "target_table": full_table_name,
                                    "warehouse": http_path_input,
                                    "status": "success",
                                    "rows_count": len(df),
                                    "query_time": start_time.isoformat()
                                })
                                st.session_state["log_table_operation"]("read", full_table_name, log_data)
                            
                            # Display success and data
                            st.success(f"Successfully read {len(df)} rows")
                            st.dataframe(df)
                        
                        except Exception as e:
                            # Create log entry for failed read
                            if "log_table_operation" in st.session_state:
                                log_data = user_info.copy() if user_info else {}
                                log_data.update({
                                    "target_table": full_table_name,
                                    "warehouse": http_path_input,
                                    "status": "failed",
                                    "error": str(e),
                                    "query_time": datetime.datetime.now().isoformat()
                                })
                                st.session_state["log_table_operation"]("read_failed", full_table_name, log_data)
                            
                            # Display error to user
                            error_msg = str(e)
                            if "403" in error_msg and "FORBIDDEN" in error_msg:
                                st.error(f"Access denied: You don't have permission to read table {full_table_name}. Please check your credentials or request access.", icon="🔒")
                            else:
                                st.error(f"Failed to read table: {error_msg}", icon="❌")
        """
    )

with tab_c:
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
                    **Permissions (app service principal)**
                    * `SELECT` on the Unity Catalog table
                    * `CAN USE` on the SQL warehouse
                    """)
    with col2:
        st.markdown("""
                    **Databricks resources**
                    * SQL warehouse
                    * Unity Catalog table
                    """)
    with col3:
        st.markdown("""
                    **Dependencies**
                    * [Databricks SDK](https://pypi.org/project/databricks-sdk/) - `databricks-sdk`
                    * [Databricks SQL Connector](https://pypi.org/project/databricks-sql-connector/) - `databricks-sql-connector`
                    * [Streamlit](https://pypi.org/project/streamlit/) - `streamlit`
                    """)