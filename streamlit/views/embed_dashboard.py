import streamlit as st
import streamlit.components.v1 as components
import requests
from databricks.sdk.core import Config

st.header("Data Visualization", divider=True)
st.subheader("AI/BI Dashboard")
st.write(
    """
    This recipe uses [Databricks AI/BI](https://www.databricks.com/product/ai-bi) to embed a dashboard into a Databricks App. 
    """
)
tab_a, tab_b, tab_c = st.tabs(["**Try it**", "**Code snippet**", "**Requirements**"])


with tab_a:
    try:
        cfg = Config()
        host = cfg.host

        token = list(cfg.authenticate().values())[0].split(" ")[1]
        url = f"{host}/api/2.0/lakeview/dashboards"
        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(url, headers=headers)
        
        # Debug: Show raw API response
        if st.checkbox("Show API debug info", value=False):
            st.json(response.json() if response.status_code == 200 else {"error": response.text})
            
        # Handle API error responses
        if response.status_code != 200:
            st.error(f"API Error ({response.status_code}): Could not retrieve dashboards. {response.text}")
            st.stop()
            
        # Parse API response safely
        dashboards = response.json()
        
        # Check if dashboards key exists and is not empty
        if not dashboards or 'dashboards' not in dashboards or not dashboards['dashboards']:
            st.warning("No dashboards found in your workspace, or your app service principal doesn't have access to any dashboards.")
            st.info("Please create dashboards in your Databricks workspace and ensure your app service principal has permission to view them.")
            st.stop()
            
        # Process dashboards data safely
        dashboard_list = dashboards.get('dashboards', [])
        dashboard_paths = {}
        
        for dashboard in dashboard_list:
            if 'display_name' in dashboard and 'dashboard_id' in dashboard:
                dashboard_paths[dashboard['display_name']] = dashboard['dashboard_id']

        # Find published dashboards
        published_dashboards = []

        for display_name, dashboard_id in dashboard_paths.items():
            try:
                published_url = f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published"
                published_response = requests.get(published_url, headers=headers)
            
                if published_response.status_code == 200:
                    published_dashboards.append((display_name, dashboard_id))
            except Exception as e:
                st.warning(f"Error checking dashboard '{display_name}': {str(e)}")
                
        final_published_dashboards = dict(published_dashboards)
        
        # Display dashboard selector if any are available
        if not final_published_dashboards:
            st.warning("No published dashboards found. Make sure your dashboards are published and accessible to your app service principal.")
            st.stop()
            
        iframe_source_temp = st.selectbox(
            "Select your AI/BI Dashboard:", [""] + list(final_published_dashboards.keys()),
            help="Dashboard list populated from your workspace using app service principal.",
        )
    
        # Display selected dashboard
        if iframe_source_temp and iframe_source_temp != "":
            dashboard_id = final_published_dashboards.get(iframe_source_temp)
            iframe_source = f"{host}/embed/dashboardsv3/{dashboard_id}"
            
            if st.checkbox("Show dashboard URL", value=False):
                st.code(iframe_source)
                
            components.iframe(src=iframe_source, width=700, height=600, scrolling=True)
            
    except Exception as e:
        st.error(f"Error loading dashboards: {str(e)}")
        st.info("This feature requires Databricks AI/BI to be enabled in your workspace and your app service principal needs permission to access dashboards.")

with tab_b:
    st.code(
        """
        import streamlit as st
        import streamlit.components.v1 as components
        import requests
        from databricks.sdk.core import Config
        
        # Get workspace information
        cfg = Config()
        host = cfg.host
        
        # Get authentication token
        token = list(cfg.authenticate().values())[0].split(" ")[1]
        
        # Call dashboards API to get available dashboards
        url = f"{host}/api/2.0/lakeview/dashboards"
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        response = requests.get(url, headers=headers)
        dashboards = response.json()
        
        # Get dashboard ID from selection
        dashboard_id = "your-dashboard-id"  # Replace with actual dashboard ID
        
        # Build embed URL
        iframe_source = f"{host}/embed/dashboardsv3/{dashboard_id}"
        
        # Display dashboard in iframe
        components.iframe(
            src=iframe_source,
            width=700,
            height=600,
            scrolling=True
        )
        """
    )

with tab_c:
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
                    **Permissions (app service principal)**
                    * `CAN VIEW` permission on the dashboard
                    """)
    with col2:
        st.markdown("""
                    **Databricks resources**
                    * SQL Warehouse
                    * Published dashboards
                    """)
    with col3:
        st.markdown("""
                    **Dependencies**
                    * [Streamlit](https://pypi.org/project/streamlit/) - `streamlit`
                    * [Requests](https://pypi.org/project/requests/) - `requests`
                    """)

    st.warning(
        "A workspace admin needs to enable dashboard embedding in the Security settings of your Databricks workspace for specific domains (e.g., databricksapps.com) or all domains for this sample to work.",
        icon="⚠️",
    ) 