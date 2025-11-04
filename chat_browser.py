import json
from typing import Dict, Any
from tools.sqlite import query_db, get_db_schema, get_patient_data, compare_dates

# Function definitions for WebLLM (same format as OpenAI)
FUNCTIONS = [
    {
        "type": "function",
        "function": {
            "name": "query_db",
            "description": "Execute a read-only SQL query against the database and return the result as a string. Only SELECT queries are allowed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL SELECT query to execute",
                    },
                    "db_path": {
                        "type": "string",
                        "description": "Path to the SQLite database file (optional, defaults to ./synthea_data.db)",
                        "default": "./synthea_data.db",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_db_schema",
            "description": "Return the schema of all tables in the database, including column names, types, and constraints.",
            "parameters": {
                "type": "object",
                "properties": {
                    "db_path": {
                        "type": "string",
                        "description": "Path to the SQLite database file (optional, defaults to ./synthea_data.db)",
                        "default": "./synthea_data.db",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_patient_data",
            "description": "Retrieve data from specified tables for a patient, optionally filtered by date range. Available tables: demographics, medications, labs, lab_tests_only, imaging, procedures, conditions, problem_list, encounters, allergies, immunizations, careplans, devices.",
            "parameters": {
                "type": "object",
                "properties": {
                    "patient_id": {
                        "type": "string",
                        "description": "Unique patient identifier",
                    },
                    "tables": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "demographics",
                                "medications",
                                "labs",
                                "lab_tests_only",
                                "imaging",
                                "procedures",
                                "conditions",
                                "problem_list",
                                "encounters",
                                "allergies",
                                "immunizations",
                                "careplans",
                                "devices",
                            ],
                        },
                        "description": "List of table names to retrieve data from",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date for filtering (YYYY-MM-DD format). Must be provided with end_date.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date for filtering (YYYY-MM-DD format). Must be provided with start_date.",
                    },
                    "db_path": {
                        "type": "string",
                        "description": "Path to the SQLite database file (optional, defaults to ./synthea_data.db)",
                        "default": "./synthea_data.db",
                    },
                },
                "required": ["patient_id", "tables"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_dates",
            "description": "Compare two dates and return information about their relationship (which is earlier, if equal, difference in days).",
            "parameters": {
                "type": "object",
                "properties": {
                    "date1": {
                        "type": "string",
                        "description": "First date in YYYY-MM-DD format",
                    },
                    "date2": {
                        "type": "string",
                        "description": "Second date in YYYY-MM-DD format",
                    },
                    "date_format": {
                        "type": "string",
                        "description": "Format of the input dates (default: %Y-%m-%d)",
                        "default": "%Y-%m-%d",
                    },
                    "db_path": {
                        "type": "string",
                        "description": "Path to the SQLite database file (optional, defaults to ./synthea_data.db)",
                        "default": "./synthea_data.db",
                    },
                },
                "required": ["date1", "date2"],
            },
        },
    },
]

# Function mapping
FUNCTION_MAP = {
    "query_db": query_db,
    "get_db_schema": get_db_schema,
    "get_patient_data": get_patient_data,
    "compare_dates": compare_dates,
}


def execute_function(function_name: str, arguments: Dict[str, Any]) -> Any:
    """Execute a function call and return the result."""
    if function_name not in FUNCTION_MAP:
        return f"Error: Function {function_name} not found."

    try:
        func = FUNCTION_MAP[function_name]
        # For compare_dates, it returns a dict, so we'll convert it to a string
        result = func(**arguments)
        if isinstance(result, dict):
            return json.dumps(result, indent=2)
        return result
    except Exception as e:
        return f"Error executing {function_name}: {str(e)}"


# Export functions for JavaScript to use
def get_functions():
    """Return the function definitions for WebLLM API."""
    return FUNCTIONS

def execute_tool(function_name: str, arguments_json: str) -> str:
    """Execute a tool/function and return the result as JSON string."""
    try:
        arguments = json.loads(arguments_json)
        result = execute_function(function_name, arguments)
        return json.dumps({"success": True, "result": str(result)})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})
