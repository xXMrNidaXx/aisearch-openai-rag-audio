import logging
import os
from pathlib import Path

from aiohttp import web
from azure.core.credentials import AzureKeyCredential
from azure.identity import AzureDeveloperCliCredential, DefaultAzureCredential
from dotenv import load_dotenv

from ragtools import attach_rag_tools
from rtmt import RTMiddleTier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("voicerag")

async def create_app():
    if not os.environ.get("RUNNING_IN_PRODUCTION"):
        logger.info("Running in development mode, loading from .env file")
        load_dotenv()

    llm_key = os.environ.get("AZURE_OPENAI_API_KEY")
    search_key = os.environ.get("AZURE_SEARCH_API_KEY")

    credential = None
    if not llm_key or not search_key:
        if tenant_id := os.environ.get("AZURE_TENANT_ID"):
            logger.info("Using AzureDeveloperCliCredential with tenant_id %s", tenant_id)
            credential = AzureDeveloperCliCredential(tenant_id=tenant_id, process_timeout=60)
        else:
            logger.info("Using DefaultAzureCredential")
            credential = DefaultAzureCredential()
    llm_credential = AzureKeyCredential(llm_key) if llm_key else credential
    search_credential = AzureKeyCredential(search_key) if search_key else credential
    
    app = web.Application()

    rtmt = RTMiddleTier(
        credentials=llm_credential,
        endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        deployment=os.environ["AZURE_OPENAI_REALTIME_DEPLOYMENT"],
        voice_choice=os.environ.get("AZURE_OPENAI_REALTIME_VOICE_CHOICE") or "alloy"
    )
    rtmt.system_message = """
You are a helpful AI assistant for the Raleigh Water Department hotline, designed to provide residents with fast, accurate answers to FAQs about water services. When a new call begins, always start by briefly introducing yourself and explaining your role.
Your responses must be clear, concise, and ideally a single short sentence suitable for audio delivery. Always follow these steps:
1. Introduce Yourself: If this is the start of a call, begin with a brief greeting such as "Hello, I'm the Raleigh Water hotline assistant, here to help with your water service questions."
2. If you detect another language spoken besides English, ask the user if they would like to switch to that language.
3. Use the RAG Tools: Use the 'search' tool to consult the knowledge base for the most current and relevant information, and use the 'report_grounding' tool to document your source (do not read this aloud).
4. Keep it Concise: Provide an answer in as short a sentence as possible. If the answer isn't in the knowledge base, say "I'm sorry, I don't have that information."
5. Maintain Confidentiality: Do not mention file names, source names, or keys in your audible responses.
6. Offer Further Assistance: If more details are needed or the question cannot be fully answered, offer to collect a callback number or connect the caller with a human operator.
7. Utility Assistance: If a resident mentions difficulty paying their water bill or asks about financial help, use the 'fill_out_utility_form' tool to help them complete an application for utility assistance. Once the form is filled out, use the 'save_utility_form' tool to save the information.
8. Prioritize Accuracy: Ensure your responses are accurate, relevant, and up-to-date.
""".strip()

    attach_rag_tools(rtmt,
        credentials=search_credential,
        search_endpoint=os.environ.get("AZURE_SEARCH_ENDPOINT"),
        search_index=os.environ.get("AZURE_SEARCH_INDEX"),
        semantic_configuration=os.environ.get("AZURE_SEARCH_SEMANTIC_CONFIGURATION") or None,
        identifier_field=os.environ.get("AZURE_SEARCH_IDENTIFIER_FIELD") or "chunk_id",
        content_field=os.environ.get("AZURE_SEARCH_CONTENT_FIELD") or "chunk",
        embedding_field=os.environ.get("AZURE_SEARCH_EMBEDDING_FIELD") or "text_vector",
        title_field=os.environ.get("AZURE_SEARCH_TITLE_FIELD") or "title",
        use_vector_query=(os.environ.get("AZURE_SEARCH_USE_VECTOR_QUERY") == "true") or True,
    )

    # Verify that all tools are attached
    logger.info("Attached tools: %s", ", ".join(rtmt.tools.keys()))

    rtmt.attach_to_app(app, "/realtime")

    current_directory = Path(__file__).parent
    app.add_routes([web.get('/', lambda _: web.FileResponse(current_directory / 'static/index.html'))])
    app.router.add_static('/', path=current_directory / 'static', name='static')
    
    return app

if __name__ == "__main__":
    host = "localhost"
    port = 8765
    web.run_app(create_app(), host=host, port=port)

