import json
import os
import re
from typing import Any, Optional
from datetime import datetime

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents.aio import SearchClient
from azure.search.documents.models import VectorizableTextQuery

from rtmt import RTMiddleTier, Tool, ToolResult, ToolResultDirection
from supabase import create_client, Client

# --------------------------------------------------------------------------------
# JSON Schemas
# --------------------------------------------------------------------------------
_search_tool_schema = {
    "type": "function",
    "name": "search",
    "description": (
        "Search the knowledge base. The knowledge base is in English, translate "
        "to and from English if needed. Results are formatted as a source name first "
        "in square brackets, followed by the text content, and a line with '-----' "
        "at the end of each result."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query"
            }
        },
        "required": ["query"],
        "additionalProperties": False
    }
}

_grounding_tool_schema = {
    "type": "function",
    "name": "report_grounding",
    "description": (
        "Report use of a source from the knowledge base as part of an answer "
        "(effectively, cite the source). Sources appear in square brackets before "
        "each knowledge base passage. Always use this tool to cite sources when "
        "responding with information from the knowledge base."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "sources": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "List of source names from last statement actually used, "
                    "do not include the ones not used to formulate a response"
                )
            }
        },
        "required": ["sources"],
        "additionalProperties": False
    }
}

_fill_out_utility_form_schema = {
  "type": "function",
  "name": "fill_out_utility_form",
  "description": "Fill out the City of Raleigh Utility Assistance Application with all relevant fields from both pages.",
  "parameters": {
    "type": "object",
    "properties": {
      "county_case_number":        { "type": "string", "description": "County Case Number" },
      "social_security_number":    { "type": "string", "description": "Social Security Number" },
      "date_of_birth":             { "type": "string", "description": "Applicant Date of Birth" },
      "first_name":                { "type": "string" },
      "middle_initial":            { "type": "string" },
      "last_name":                 { "type": "string" },
      "suffix":                    { "type": "string", "description": "Jr, Sr, etc." },
      "residence_address_street":  { "type": "string" },
      "residence_address_city":    { "type": "string" },
      "residence_address_zip":     { "type": "string" },
      "mailing_address_street":    { "type": "string" },
      "mailing_address_city":      { "type": "string" },
      "mailing_address_zip":       { "type": "string" },
      "phone_number":              { "type": "string" },
      "email_address":             { "type": "string" },
      "household_members": {
        "type": "array",
        "description": "List of household members including the applicant",
        "items": {
          "type": "object",
          "properties": {
            "name":                   { "type": "string" },
            "date_of_birth":          { "type": "string" },
            "social_security_number": { "type": "string" },
            "employer":              { "type": "string" },
            "income_description":    { "type": "string", "description": "Wages, salary, tips, bonus, etc." },
            "income_frequency":      { "type": "string", "description": "How often paid (weekly, biweekly, monthly...)" },
            "date_last_received":    { "type": "string" }
          }
        }
      },
      "additional_income_sources": {
        "type": "array",
        "description": "Indicate all additional income sources for each household member",
        "items": {
          "type": "object",
          "properties": {
            "source_type": {
              "type": "string",
              "enum": [
                "Work First Benefits", "SSI Benefits", "Social Security Benefits",
                "Veterans Benefits", "Unemployment Benefits", "Pandemic Unemployment",
                "Child Support", "Worker's Compensation", "Severance Pay",
                "Retirement/Pension", "Armed Forces/Military Pay", "Self-employment",
                "Adoption Assistance Payments", "Welfare Assistance", "Rental Income",
                "Interest/Dividends", "Independent Contractor", "Re-occurring Cash Gifts",
                "Asset Income", "Other"
              ]
            },
            "who":                 { "type": "string", "description": "Which household member?" },
            "amount":             { "type": "string" },
            "date_last_received": { "type": "string" }
          }
        }
      },
      "additional_family_members": {
        "type": "array",
        "description": "Any additional family members or income not included on page 1",
        "items": {
          "type": "object",
          "properties": {
            "name":                   { "type": "string" },
            "date_of_birth":          { "type": "string" },
            "social_security_number": { "type": "string" },
            "employer":              { "type": "string" },
            "income_description":    { "type": "string" },
            "income_frequency":      { "type": "string" },
            "date_last_received":    { "type": "string" }
          }
        }
      },
      "are_you_currently_receiving": {
        "type": "object",
        "description": "Check if you are currently receiving these assistance programs",
        "properties": {
          "energy_assistance_cip_lieap": { "type": "boolean", "description": "CIP / LIEAP" },
          "food_and_nutrition_fns_snap": { "type": "boolean", "description": "FNS / SNAP / Food Stamps" },
          "medicaid":                    { "type": "boolean" },
          "work_first":                  { "type": "boolean" }
        }
      },
      "have_you_received_raleigh_water_assistance": { "type": "boolean" },
      "most_recent_raleigh_water_assistance_date":  { "type": "string", "description": "If yes, when?" },
      "are_you_renting_your_home_apartment": {
        "type": "string",
        "enum": ["Yes", "No", "Other"]
      },
      "amount_due":     { "type": "string" },
      "service_current_on": { "type": "boolean" },
      "city_of_raleigh_utility_account_number": { "type": "string" },
      "name_on_account":                        { "type": "string" },
      "would_you_like_to_register_to_vote": { "type": "boolean", "description": "Yes/No. If not checked, user is considered not interested." },
      "signature_applicant": { "type": "string" },
      "signature_date":      { "type": "string" }
    },
    "required": [],
    "additionalProperties": False
  }
}

_save_utility_form_schema = {
    "type": "function",
    "name": "save_utility_form",
    "description": "Save the completed Utility Assistance Application form into a Supabase table.",
    "parameters": {
        "type": "object",
        "properties": {
            "form_data": {
                "type": "object",
                "description": "A dictionary of all fields from the completed form.",
                "additionalProperties": True
            }
        },
        "required": ["form_data"],
        "additionalProperties": False
    }
}

# --------------------------------------------------------------------------------
# Utility Parsers
# --------------------------------------------------------------------------------

def try_parse_date(date_str: Optional[str]) -> Optional[str]:
    """
    Attempt to parse a date in 'YYYY-MM-DD' format. Returns an ISO 8601 string 
    (YYYY-MM-DD) if successful, or None if missing or invalid.
    """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")  # ensure correct format
    except ValueError:
        return None

# --------------------------------------------------------------------------------
# Helper Functions (Tool Implementations)
# --------------------------------------------------------------------------------

async def _fill_out_utility_form(args: Any) -> ToolResult:
    """
    Merges the user-provided arguments into a structured representation
    of the Utility Assistance Application form, ensuring all fields are filled.
    """
    default_form = {
        "county_case_number": "", "social_security_number": "", "date_of_birth": "",
        "first_name": "", "middle_initial": "", "last_name": "", "suffix": "",
        "residence_address_street": "", "residence_address_city": "", "residence_address_zip": "",
        "mailing_address_street": "", "mailing_address_city": "", "mailing_address_zip": "",
        "phone_number": "", "email_address": "",
        "household_members": [], "additional_income_sources": [], "additional_family_members": [],
        "are_you_currently_receiving": {
            "energy_assistance_cip_lieap": False, "food_and_nutrition_fns_snap": False,
            "medicaid": False, "work_first": False
        },
        "have_you_received_raleigh_water_assistance": False,
        "most_recent_raleigh_water_assistance_date": "",
        "are_you_renting_your_home_apartment": "",
        "amount_due": "", "service_current_on": False,
        "city_of_raleigh_utility_account_number": "", "name_on_account": "",
        "would_you_like_to_register_to_vote": False,
        "signature_applicant": "", "signature_date": ""
    }
    
    filled_form = {**default_form, **dict(args)}
    return ToolResult(json.dumps(filled_form), destination=ToolResultDirection.TO_SERVER)

# Create a single Supabase client at module level
SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if (SUPABASE_URL and SUPABASE_KEY) else None

async def _save_utility_form(args: Any) -> ToolResult:
    """
    Insert the 'form_data' into our 'raleigh_utility_forms' table, mapping 
    each field to a corresponding column, including parsing of date strings.

    For arrays/objects (like household_members), we store them directly as JSONB.
    """
    if not supabase:
        return ToolResult(
            json.dumps({
                "status": "error",
                "message": "Supabase client is not configured. Check your SUPABASE_URL/KEY environment."
            }),
            destination=ToolResultDirection.TO_SERVER
        )

    try:
        form_data = json.loads(args.get("form_data", "{}"))
    except json.JSONDecodeError:
        return ToolResult(
            json.dumps({
                "status": "error",
                "message": "Invalid JSON in form_data"
            }),
            destination=ToolResultDirection.TO_SERVER
        )

    # Map raw strings + booleans + arrays to columns in the table
    mapped_data = {
        "county_case_number": form_data.get("county_case_number"),
        "social_security_number": form_data.get("social_security_number"),
        "date_of_birth": try_parse_date(form_data.get("date_of_birth")),
        "first_name": form_data.get("first_name"),
        "middle_initial": form_data.get("middle_initial"),
        "last_name": form_data.get("last_name"),
        "suffix": form_data.get("suffix"),
        "residence_address_street": form_data.get("residence_address_street"),
        "residence_address_city": form_data.get("residence_address_city"),
        "residence_address_zip": form_data.get("residence_address_zip"),
        "mailing_address_street": form_data.get("mailing_address_street"),
        "mailing_address_city": form_data.get("mailing_address_city"),
        "mailing_address_zip": form_data.get("mailing_address_zip"),
        "phone_number": form_data.get("phone_number"),
        "email_address": form_data.get("email_address"),
        "household_members": form_data.get("household_members"),
        "additional_income_sources": form_data.get("additional_income_sources"),
        "additional_family_members": form_data.get("additional_family_members"),
        "are_you_currently_receiving": form_data.get("are_you_currently_receiving"),
        "have_you_received_raleigh_water_assistance": form_data.get("have_you_received_raleigh_water_assistance"),
        "most_recent_raleigh_water_assistance_date": try_parse_date(form_data.get("most_recent_raleigh_water_assistance_date")),
        "are_you_renting_your_home_apartment": form_data.get("are_you_renting_your_home_apartment"),
        "amount_due": form_data.get("amount_due"),
        "service_current_on": form_data.get("service_current_on"),
        "city_of_raleigh_utility_account_number": form_data.get("city_of_raleigh_utility_account_number"),
        "name_on_account": form_data.get("name_on_account"),
        "would_you_like_to_register_to_vote": form_data.get("would_you_like_to_register_to_vote"),
        "signature_applicant": form_data.get("signature_applicant"),
        "signature_date": try_parse_date(form_data.get("signature_date"))
    }

    try:
        response = supabase.table("raleigh_utility_forms").insert(mapped_data).execute()
        return ToolResult(
            json.dumps({
                "status": "success",
                "message": "Utility form data inserted successfully.",
                "inserted_rows": response.data
            }),
            destination=ToolResultDirection.TO_SERVER
        )
    except Exception as e:
        return ToolResult(
            json.dumps({
                "status": "error",
                "message": f"Error inserting data: {str(e)}",
            }),
            destination=ToolResultDirection.TO_SERVER
        )

KEY_PATTERN = re.compile(r'^[a-zA-Z0-9_=\-]+$')

async def _search_tool(
    search_client: SearchClient,
    semantic_configuration: str | None,
    identifier_field: str,
    content_field: str,
    embedding_field: str,
    use_vector_query: bool,
    args: Any
) -> ToolResult:
    print(f"Searching for '{args['query']}' in the knowledge base.")
    vector_queries = []
    if use_vector_query:
        vector_queries.append(VectorizableTextQuery(
            text=args['query'], 
            k_nearest_neighbors=50, 
            fields=embedding_field
        ))
    search_results = await search_client.search(
        search_text=args["query"], 
        query_type="semantic" if semantic_configuration else "simple",
        semantic_configuration_name=semantic_configuration,
        top=5,
        vector_queries=vector_queries,
        select=", ".join([identifier_field, content_field])
    )
    result = ""
    async for r in search_results:
        result += f"[{r[identifier_field]}]: {r[content_field]}\n-----\n"
    return ToolResult(result, destination=ToolResultDirection.TO_SERVER)

async def _report_grounding_tool(
    search_client: SearchClient,
    identifier_field: str,
    title_field: str,
    content_field: str,
    args: Any
) -> ToolResult:
    sources = [s for s in args["sources"] if KEY_PATTERN.match(s)]
    list_of_sources = " OR ".join(sources)
    print(f"Grounding source: {list_of_sources}")

    search_results = await search_client.search(
        search_text=list_of_sources,
        search_fields=[identifier_field],
        select=[identifier_field, title_field, content_field],
        top=len(sources),
        query_type="full"
    )
    
    docs = []
    async for r in search_results:
        docs.append({
            "chunk_id": r[identifier_field],
            "title": r[title_field],
            "chunk": r[content_field]
        })
    return ToolResult(json.dumps({"sources": docs}), destination=ToolResultDirection.TO_CLIENT)

# --------------------------------------------------------------------------------
# attach_rag_tools
# --------------------------------------------------------------------------------
def attach_rag_tools(
    rtmt: RTMiddleTier,
    credentials: AzureKeyCredential | DefaultAzureCredential,
    search_endpoint: str,
    search_index: str,
    semantic_configuration: str | None,
    identifier_field: str,
    content_field: str,
    embedding_field: str,
    title_field: str,
    use_vector_query: bool
) -> None:
    """
    Attaches all standard RAG tools plus our new form-filling and supabase-saving tools.
    """
    if not isinstance(credentials, AzureKeyCredential):
        credentials.get_token("https://search.azure.com/.default")  # warm up token

    # Create the Azure Search client
    search_client = SearchClient(
        search_endpoint,
        search_index,
        credentials,
        user_agent="RTMiddleTier"
    )

    # 1) Search tool
    rtmt.tools["search"] = Tool(
        schema=_search_tool_schema,
        target=lambda args: _search_tool(
            search_client,
            semantic_configuration,
            identifier_field,
            content_field,
            embedding_field,
            use_vector_query,
            args
        )
    )

    # 2) Grounding tool
    rtmt.tools["report_grounding"] = Tool(
        schema=_grounding_tool_schema,
        target=lambda args: _report_grounding_tool(
            search_client,
            identifier_field,
            title_field,
            content_field,
            args
        )
    )

    # 3) Fill-out tool
    rtmt.tools["fill_out_utility_form"] = Tool(
        schema=_fill_out_utility_form_schema,
        target=_fill_out_utility_form
    )

    # 4) Supabase Save tool (with data mapping)
    rtmt.tools["save_utility_form"] = Tool(
        schema=_save_utility_form_schema,
        target=_save_utility_form
    )

    print(f"Attached tools: {', '.join(rtmt.tools.keys())}")

