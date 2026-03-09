from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from src.config import AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, AZURE_CHAT_DEPLOYMENT, AZURE_OPENAI_API_VERSION
from src.retrieval import get_retriever

def get_qa_chain():
    """Returns a LangChain RAG chain with a strict system prompt."""
    llm = ChatOpenAI(
        base_url=AZURE_OPENAI_ENDPOINT,
        model=AZURE_CHAT_DEPLOYMENT,
        api_key=AZURE_OPENAI_API_KEY,
        temperature=0
    )
    
    retriever = get_retriever()
    
    # Strict prompt to prevent hallucination on non-technical questions
    system_prompt = """You are a highly capable technical assistant.
Your job is to answer questions related to technical documentation (VMware, Nutanix, OpenShift, etc.).
You must answer the user's question using ONLY the provided context.
If the question is completely unrelated to technical topics or the provided context (e.g., asking for a poem, general knowledge, non-technical queries), you MUST gracefully decline by saying:
"I am a technical assistant and can only answer questions related to the technical documentation I have been provided. I cannot help with that query."
Do not attempt to make up an answer if the context does not contain the necessary information.

Context:
{context}

Question:
{question}
"""
    
    prompt = ChatPromptTemplate.from_template(system_prompt)
    
    def format_docs(docs):
        return "\n\n".join(doc.page_content for doc in docs)
    
    rag_chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    
    return rag_chain

def generate_answer(query: str):
    """Generates an answer using the RAG pipeline."""
    rag_chain = get_qa_chain()
    return rag_chain.invoke(query)
