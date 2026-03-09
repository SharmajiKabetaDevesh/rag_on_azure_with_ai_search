import os
from langchain_community.document_loaders import WebBaseLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores.azuresearch import AzureSearch
from azure.storage.blob import BlobServiceClient
from src.config import AZURE_SEARCH_ENDPOINT, AZURE_SEARCH_API_KEY, AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_CONTAINER_NAME, AZURE_OPENAI_ENDPOINT, AZURE_EMBEDDING_DEPLOYMENT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_API_VERSION

# Sample technical documentation URLs to ingest
TECH_DOCS_URLS = [
    "https://docs.vmware.com/en/VMware-vSphere/8.0/vsphere-esxi-vcenter-80-installation-setup-guide.pdf",
    "https://portal.nutanix.com/page/documents/details?targetId=Web-Console-Guide-Prism-v6_7:Web-Console-Guide-Prism-v6_7",
    "https://docs.openshift.com/container-platform/4.14/architecture/architecture.html"
]

def load_documents(urls):
    """Loads documents from provided URLs using WebBaseLoader."""
    # Note: In a real-world scenario we might need more advanced scrapers (e.g. Playwright) 
    # for JS-heavy sites or specific PDF loaders.
    print(f"Loading {len(urls)} URLs...")
    loader = WebBaseLoader(urls)
    docs = loader.load()
    print(f"Loaded {len(docs)} documents.")
    return docs

def split_documents(docs):
    """Splits documents into smaller chunks for embeddings."""
    print("Splitting documents...")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
    )
    chunks = text_splitter.split_documents(docs)
    print(f"Split down to {len(chunks)} chunks.")
    return chunks

def ingest_to_azure_search(chunks):
    """Embeds the chunks and upserts them into Azure AI Search."""
    print("Setting up Azure Search Vector Store...")
    
    embeddings = OpenAIEmbeddings(
        base_url=AZURE_OPENAI_ENDPOINT,
        model=AZURE_EMBEDDING_DEPLOYMENT,
        api_key=AZURE_OPENAI_API_KEY
    )

    index_name = "tech-docs-index"
    
    # Setup Azure Search VectorStore from LangChain
    vector_store = AzureSearch(
        azure_search_endpoint=AZURE_SEARCH_ENDPOINT,
        azure_search_key=AZURE_SEARCH_API_KEY,
        index_name=index_name,
        embedding_function=embeddings.embed_query,
    )
    
    print("Adding documents to Azure AI Search...")
    vector_store.add_documents(documents=chunks)
    print("Successfully ingested documents into Azure AI Search.")

def save_to_azure_blob(docs):
    """Saves raw documents to Azure Blob Storage for archival/reference."""
    if not AZURE_STORAGE_CONNECTION_STRING or not AZURE_STORAGE_CONTAINER_NAME:
        print("Skipping Azure Blob Storage upload: Credentials not provided.")
        return
        
    print("Uploading raw documents to Azure Blob Storage...")
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER_NAME)
        
        # Create container if it doesn't exist
        if not container_client.exists():
            container_client.create_container()
            
        for i, doc in enumerate(docs):
            source_url = doc.metadata.get('source', f'doc_{i}')
            # Create a safe filename from URL
            safe_name = "".join([c if c.isalnum() else "_" for c in source_url]) + ".txt"
            
            blob_client = container_client.get_blob_client(safe_name)
            blob_client.upload_blob(doc.page_content, overwrite=True)
            
        print("Successfully uploaded raw documents to Azure Blob Storage.")
    except Exception as e:
        print(f"Failed to upload to Azure Blob Storage: {e}")

def run_ingestionPipeline():
    docs = load_documents(TECH_DOCS_URLS)
    save_to_azure_blob(docs)
    chunks = split_documents(docs)
    ingest_to_azure_search(chunks)

if __name__ == "__main__":
    run_ingestionPipeline()
