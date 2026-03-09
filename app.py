import streamlit as st
import traceback

# Optional: suppress some warnings
import warnings
warnings.filterwarnings("ignore")

# Initialize page
st.set_page_config(page_title="Technical Assistant RAG", page_icon="🤖", layout="wide")

st.title("🤖 Azure RAG Technical Assistant")
st.markdown("Ask questions related to VMware, Nutanix, or OpenShift documentation.")

# Sidebar for Setup / Info
with st.sidebar:
    st.header("About")
    st.write("This application uses native Azure AI Search (Hybrid + RRF) for retrieval, and OpenAI GPT-4o for generation.")
    
    st.header("Actions")
    if st.button("Run Data Ingestion"):
        try:
            with st.spinner("Ingesting data into Azure AI Search..."):
                from src.ingestion import run_ingestionPipeline
                run_ingestionPipeline()
            st.success("Ingestion complete!")
        except Exception as e:
            st.error(f"Error during ingestion: {str(e)}")
            st.code(traceback.format_exc())

# Chat history initialization
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "How can I help you with your technical questions today?"}
    ]

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# React to user input
if prompt := st.chat_input("E.g., How do I install vSphere?"):
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Assistant response
    with st.chat_message("assistant"):
        response_placeholder = st.empty()
        
        try:
            with st.spinner("Thinking..."):
                from src.generation import generate_answer
                from src.retrieval import retrieve_documents
                
                # Fetching response
                answer = generate_answer(prompt)
                
                # Displaying response
                response_placeholder.markdown(answer)
                st.session_state.messages.append({"role": "assistant", "content": answer})
                
                # Optionally, show retrieved docs used for context
                with st.expander("Show Retrieved Sources"):
                    docs = retrieve_documents(prompt)
                    if docs:
                        for idx, doc in enumerate(docs):
                            st.markdown(f"**Source {idx + 1}**")
                            st.write(doc.page_content)
                            if doc.metadata:
                                st.write(f"Metadata: {doc.metadata}")
                            st.markdown("---")
                    else:
                        st.write("No sources found.")
                        
        except Exception as e:
            st.error(f"Error generating answer: {str(e)}")
            st.code(traceback.format_exc())
