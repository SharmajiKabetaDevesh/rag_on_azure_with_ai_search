import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Azure Services Configuration
AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
AZURE_SEARCH_API_KEY = os.getenv("AZURE_SEARCH_API_KEY")

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

# AI Services Configuration
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT=os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION=os.getenv("AZURE_OPENAI_API_VERSION")
AZURE_EMBEDDING_DEPLOYMENT=os.getenv("AZURE_EMBEDDING_DEPLOYMENT")
AZURE_CHAT_DEPLOYMENT=os.getenv("AZURE_CHAT_DEPLOYMENT")

def validate_config():
    """Validates that all essential environment variables are set."""
    missing_vars = []
    
    if not AZURE_SEARCH_ENDPOINT:
        missing_vars.append("AZURE_SEARCH_ENDPOINT")
    if not AZURE_SEARCH_API_KEY:
        missing_vars.append("AZURE_SEARCH_API_KEY")
    if not AZURE_OPENAI_API_KEY:
        missing_vars.append("AZURE_OPENAI_API_KEY")
    
    if missing_vars:
        print(f"ERROR: Missing essential environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file.")
        sys.exit(1)

validate_config()
