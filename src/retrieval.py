from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores.azuresearch import AzureSearch
from src.config import AZURE_SEARCH_ENDPOINT, AZURE_SEARCH_API_KEY, AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, AZURE_EMBEDDING_DEPLOYMENT, AZURE_OPENAI_API_VERSION

def get_retriever():
    """Returns a retriever using Azure Search's native Hybrid Search (which employs RRF)."""
    embeddings = OpenAIEmbeddings(
        base_url=AZURE_OPENAI_ENDPOINT,
        model=AZURE_EMBEDDING_DEPLOYMENT,
        api_key=AZURE_OPENAI_API_KEY
    )
    index_name = "tech-docs-index"
    
    # Initialize Azure Search vector store
    vector_store = AzureSearch(
        azure_search_endpoint=AZURE_SEARCH_ENDPOINT,
        azure_search_key=AZURE_SEARCH_API_KEY,
        index_name=index_name,
        embedding_function=embeddings.embed_query,
    )
    
    # Setting search_type to 'hybrid' leverages Azure Search's native Reciprocal Rank Fusion (RRF)
    # combining keyword and vector search.
    retriever = vector_store.as_retriever(
        search_type="hybrid"
    )
    
    return retriever

def retrieve_documents(query: str):
    """Retrieves and reranks documents based on the query."""
    retriever = get_retriever()
    docs = retriever.invoke(query)
    return docs
