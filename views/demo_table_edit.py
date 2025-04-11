import pandas as pd
import streamlit as st
import databricks.sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient

st.header(body="Demo", divider=True)
st.subheader("Table Edit Demo")
st.write(
    "This demo shows how to read, edit, and write back data stored in a small Unity Catalog table "
    "using the [Databricks SQL Connector]"
    "(https://docs.databricks.com/en/dev-tools/python-sql-connector.html)."
)

cfg = Config()
w = WorkspaceClient()

# Get warehouses and catalogs
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


def read_table(table_name: str, conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall_arrow().to_pandas()


def insert_overwrite_table(table_name: str, df: pd.DataFrame, conn):
    progress = st.empty()
    with conn.cursor() as cursor:
        rows = list(df.itertuples(index=False))
        values = ",".join([f"({','.join(map(repr, row))})" for row in rows])
        with progress:
            st.info("Calling Databricks SQL...")
        cursor.execute(f"INSERT OVERWRITE {table_name} VALUES {values}")
    progress.empty()
    st.success("Changes saved")


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
            http_path = warehouse_paths[http_path_input]
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

            conn = get_connection(http_path)
            original_df = read_table(full_table_name, conn)
            edited_df = st.data_editor(original_df, num_rows="dynamic", hide_index=True)

            df_diff = pd.concat([original_df, edited_df]).drop_duplicates(keep=False)
            if not df_diff.empty:
                if st.button("Save changes"):
                    # Log table update operation
                    if "log_table_operation" in st.session_state:
                        # Add information about changes
                        user_info_with_changes = user_info.copy()
                        user_info_with_changes.update({
                            "rows_modified": len(df_diff) // 2,  # Divide by 2 as each change is counted twice in diff
                            "total_rows": len(edited_df)
                        })
                        # Log the update operation
                        st.session_state["log_table_operation"]("update", full_table_name, user_info_with_changes)
                    
                    insert_overwrite_table(full_table_name, edited_df, conn)
    else:
        st.warning("Please select warehouse, catalog, schema and table to edit data.")


with tab_b:
    st.code(
        """
        import pandas as pd
        import streamlit as st
        import databricks.sql
        from databricks.sdk.core import Config
        from databricks.sdk import WorkspaceClient

        # Initialize configuration and workspace client
        cfg = Config()
        w = WorkspaceClient()

        # Get warehouses and catalogs
        warehouses = w.warehouses.list()
        warehouse_paths = {wh.name: wh.odbc_params.path for wh in warehouses}
        catalogs = w.catalogs.list()

        # Cache SQL connection
        @st.cache_resource
        def get_connection(http_path):
            return databricks.sql.connect(
                server_hostname=cfg.host,
                http_path=http_path,
                credentials_provider=lambda: cfg.authenticate,
            )

        # Read table data
        def read_table(table_name: str, conn) -> pd.DataFrame:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name}")
                return cursor.fetchall_arrow().to_pandas()

        # Write data back to table
        def insert_overwrite_table(table_name: str, df: pd.DataFrame, conn):
            progress = st.empty()
            with conn.cursor() as cursor:
                rows = list(df.itertuples(index=False))
                values = ",".join([f"({','.join(map(repr, row))})" for row in rows])
                with progress:
                    st.info("Calling Databricks SQL...")
                cursor.execute(f"INSERT OVERWRITE {table_name} VALUES {values}")
            progress.empty()
            st.success("Changes saved")

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
                http_path = warehouse_paths[http_path_input]
                full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
                
                conn = get_connection(http_path)
                original_df = read_table(full_table_name, conn)
                edited_df = st.data_editor(original_df, num_rows="dynamic", hide_index=True)

                df_diff = pd.concat([original_df, edited_df]).drop_duplicates(keep=False)
                if not df_diff.empty:
                    if st.button("Save changes"):
                        insert_overwrite_table(full_table_name, edited_df, conn)
        """
    )

with tab_c:
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            """
            **Permissions (app service principal)**
            * `MODIFY` on the Unity Catalog table
            * `CAN USE` on the SQL warehouse
            """
        )
    with col2:
        st.markdown(
            """
            **Databricks resources**
            * SQL warehouse
            * Unity Catalog table
            """
        )
    with col3:
        st.markdown(
            """
            **Dependencies**
            * [Databricks SDK](https://pypi.org/project/databricks-sdk/) - `databricks-sdk`
            * [Databricks SQL Connector](https://pypi.org/project/databricks-sql-connector/) - `databricks-sql-connector`
            * [Pandas](https://pypi.org/project/pandas/) - `pandas`
            * [Streamlit](https://pypi.org/project/streamlit/) - `streamlit`
            """
        ) 