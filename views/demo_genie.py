import streamlit as st
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

st.header(body="Demo", divider=True)
st.subheader("Genie AI Assistant Demo")
st.write("This is a demonstration page for the Databricks Genie AI Assistant, showing how to use Mosaic AI for text generation and conversation.")

w = WorkspaceClient()

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! I'm Genie AI Assistant. How can I help you today?"}
    ]

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Receive user input
if prompt := st.chat_input("Enter your question..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message on the interface
    with st.chat_message("user"):
        st.markdown(prompt)
        
    # Display thinking status
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        message_placeholder.markdown("Thinking...")
        
        try:
            # Get available Serving endpoints
            endpoints = w.serving_endpoints.list()
            
            # Check if endpoints are available
            if not endpoints:
                response_message = "No available AI model service endpoints found. Please ensure you've deployed an LLM model on Databricks."
            else:
                # Select the first available endpoint (in a real application, more specific selection logic might be needed)
                selected_model = endpoints[0].name
                
                # Call the AI model
                response = w.serving_endpoints.query(
                    name=selected_model,
                    messages=[
                        ChatMessage(
                            role=ChatMessageRole.SYSTEM,
                            content="You are a helpful AI assistant focused on data science and machine learning.",
                        ),
                        *[ChatMessage(role=ChatMessageRole.USER if msg["role"] == "user" else ChatMessageRole.ASSISTANT, 
                                     content=msg["content"]) 
                                     for msg in st.session_state.messages[-3:] if msg["role"] != "system"]
                    ],
                    temperature=0.7,
                )
                
                # Extract text from response
                response_message = response.as_dict().get("choices", [{}])[0].get("message", {}).get("content", "Unable to get a response")
        
        except Exception as e:
            response_message = f"Error calling AI model: {str(e)}\n\nPlease ensure you are connected to the Databricks workspace and have permission to access the AI model service."
        
        # Update message placeholder
        message_placeholder.markdown(response_message)
    
    # Add assistant's reply to chat history
    st.session_state.messages.append({"role": "assistant", "content": response_message})

# Information and code example tabs
tab_info, tab_code = st.tabs(["**Feature Description**", "**Code Sample**"])

with tab_info:
    st.markdown("""
    ### Genie AI Assistant Features
    
    This demo shows how to integrate Databricks Mosaic AI large language models into your application:
    
    1. **Chat Interface** - Create an interactive conversation experience using Streamlit's chat components
    2. **Session Management** - Maintain conversation history to provide context-aware responses
    3. **Model Integration** - Use the Databricks SDK to call LLM models deployed on Mosaic AI
    4. **Custom Instructions** - Use system prompts to customize AI assistant behavior and response style
    
    To use this feature, you need to deploy a large language model endpoint on Databricks that supports chat functionality.
    """)

with tab_code:
    st.code(
        """
        import streamlit as st
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        
        # Initialize Workspace client
        w = WorkspaceClient()
        
        # Initialize session state
        if "messages" not in st.session_state:
            st.session_state.messages = [
                {"role": "assistant", "content": "Hello! I'm Genie AI Assistant. How can I help you today?"}
            ]
        
        # Display chat history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        # Receive user input
        if prompt := st.chat_input("Enter your question..."):
            # Add user message to chat history
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Call AI model and display response
            with st.chat_message("assistant"):
                # Call Mosaic AI model endpoint
                response = w.serving_endpoints.query(
                    name="your_model_endpoint_name",
                    messages=[
                        ChatMessage(
                            role=ChatMessageRole.SYSTEM,
                            content="You are a helpful AI assistant focused on data science and machine learning.",
                        ),
                        *[ChatMessage(role=ChatMessageRole.USER if msg["role"] == "user" else ChatMessageRole.ASSISTANT, 
                                    content=msg["content"]) 
                                    for msg in st.session_state.messages[-3:] if msg["role"] != "system"]
                    ],
                    temperature=0.7,
                )
                
                # Extract response text from the result
                response_message = response.as_dict().get("choices", [{}])[0].get("message", {}).get("content", "")
                st.markdown(response_message)
            
            # Add assistant's reply to chat history
            st.session_state.messages.append({"role": "assistant", "content": response_message})
        """
    ) 