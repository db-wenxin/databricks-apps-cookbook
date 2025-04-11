import streamlit as st
import databricks.sql
from databricks.sdk import WorkspaceClient
import pandas as pd

st.header(body="Demo", divider=True)
st.subheader("Delta Table Demo")
st.write(
    "This is a demonstration page for Delta Table functionality, showing how to connect to and work with Delta tables in Databricks."
)

# Initialize Workspace client
w = WorkspaceClient()

@st.cache_resource
def get_connection(http_path):
    return databricks.sql.connect(
        server_hostname=w.config.host,
        http_path=http_path,
        credentials_provider=lambda: w.config.authenticate,
    )

def read_delta_table(table_name, conn):
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name} LIMIT 100"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

tab_a, tab_b, tab_c = st.tabs(["**Demo**", "**Code Sample**", "**Requirements**"])

with tab_a:
    http_path_input = st.text_input(
        "Enter Databricks HTTP Path:", placeholder="/sql/1.0/warehouses/xxxxxx"
    )

    table_name = st.text_input(
        "Specify Unity Catalog table name:", placeholder="catalog.schema.table"
    )

    if http_path_input and table_name:
        try:
            conn = get_connection(http_path_input)
            df = read_delta_table(table_name, conn)
            st.success("Successfully connected and read the table!")
            st.dataframe(df)
            
            # Add some basic statistics
            st.subheader("Table Statistics")
            st.write(f"Rows: {len(df)}")
            st.write(f"Columns: {len(df.columns)}")
            
            # Display column information
            st.subheader("Column Information")
            col_info = pd.DataFrame({
                "Column Name": df.columns,
                "Data Type": df.dtypes,
                "Non-Null Count": df.count().values
            })
            st.dataframe(col_info)
            
        except Exception as e:
            st.error(f"Connection or query error: {str(e)}")

with tab_b:
    st.code(
        """
        import streamlit as st
        import databricks.sql
        from databricks.sdk import WorkspaceClient
        import pandas as pd

        # Initialize Workspace client
        w = WorkspaceClient()

        # Create connection function
        @st.cache_resource
        def get_connection(http_path):
            return databricks.sql.connect(
                server_hostname=w.config.host,
                http_path=http_path,
                credentials_provider=lambda: w.config.authenticate,
            )

        # Function to read Delta table
        def read_delta_table(table_name, conn):
            with conn.cursor() as cursor:
                query = f"SELECT * FROM {table_name} LIMIT 100"
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()

        # User input
        http_path_input = st.text_input(
            "Enter Databricks HTTP Path:", placeholder="/sql/1.0/warehouses/xxxxxx"
        )

        table_name = st.text_input(
            "Specify Unity Catalog table name:", placeholder="catalog.schema.table"
        )

        # Execute query and display results
        if http_path_input and table_name:
            try:
                conn = get_connection(http_path_input)
                df = read_delta_table(table_name, conn)
                st.dataframe(df)
            except Exception as e:
                st.error(f"Connection or query error: {str(e)}")
        """
    )

with tab_c:
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
                    **Permission Requirements**
                    * `SELECT` - Access to Unity Catalog table
                    * `CAN USE` - Permission to use SQL warehouse
                    """)
    with col2:
        st.markdown("""
                    **Databricks Resources**
                    * SQL Warehouse
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