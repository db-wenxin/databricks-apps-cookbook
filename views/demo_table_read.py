import streamlit as st
import databricks.sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient

st.header(body="Demo", divider=True)
st.subheader("Table Read Demo")
st.write(
    "This demo shows how to read a Unity Catalog table using the [Databricks SQL Connector](https://docs.databricks.com/en/dev-tools/python-sql-connector.html)."
)

cfg = Config()

w = WorkspaceClient()
warehouses = w.warehouses.list()
warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}
catalogs = w.catalogs.list()

# Get user information from HTTP headers
headers = st.context.headers
user_info = {
    "username": headers.get("X-Forwarded-Preferred-Username", "unknown"),
    "email": headers.get("X-Forwarded-Email", "unknown"),
    "ip_address": headers.get("X-Real-Ip", "unknown")
}

@st.cache_resource
def get_connection(http_path):
    return databricks.sql.connect(
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
                    http_path = warehouse_paths[http_path_input]
                    conn = get_connection(http_path)
                    df = read_table(full_table_name, conn)
                    
                    # Log table read operation
                    if "log_table_operation" in st.session_state:
                        st.session_state["log_table_operation"]("read", full_table_name, user_info)
                    
                    st.success(f"Successfully read {len(df)} rows from {full_table_name}")
                    st.dataframe(df)


with tab_b:
    st.code(
        """
        import streamlit as st
        import databricks.sql
        from databricks.sdk.core import Config
        from databricks.sdk import WorkspaceClient

        # Initialize configuration
        cfg = Config()

        # Initialize workspace client and get resources
        w = WorkspaceClient()
        warehouses = w.warehouses.list()
        warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}
        catalogs = w.catalogs.list()

        # Create connection function
        @st.cache_resource
        def get_connection(http_path):
            return databricks.sql.connect(
                server_hostname=cfg.host,
                http_path=http_path,
                credentials_provider=lambda: cfg.authenticate,
            )

        # Function to read table
        def read_table(table_name, conn):
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()

        # Helper functions to get schemas and tables
        def get_schema_names(catalog_name):
            schemas = w.schemas.list(catalog_name=catalog_name)
            return [schema.name for schema in schemas]

        def get_table_names(catalog_name, schema_name):
            tables = w.tables.list(catalog_name=catalog_name, schema_name=schema_name)
            return [table.name for table in tables]

        # UI for selecting resources
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
                
                # Add Read button - data is only fetched when the button is clicked
                if st.button("Read Data"):
                    with st.spinner(f"Reading data from {full_table_name}..."):
                        http_path = warehouse_paths[http_path_input]
                        conn = get_connection(http_path)
                        df = read_table(full_table_name, conn)
                        st.success(f"Successfully read {len(df)} rows from {full_table_name}")
                        st.dataframe(df)
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
                    * [Pandas](https://pypi.org/project/pandas/) - `pandas`
                    """) 