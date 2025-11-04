import os
import json
import argparse
from typing import List, Dict, Any
from openai import OpenAI
from dotenv import load_dotenv

from tools.sqlite import query_db, get_db_schema, get_patient_data, compare_dates

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Function definitions for OpenAI
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


def chat_loop(model: str = "gpt-4o"):
    """Main chat loop with function calling support."""
    messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": "You are a helpful healthcare assistant that can query patient data and medical records from a SQLite database. You can help users understand patient information, analyze medical data, and answer questions about healthcare records.",
        }
    ]

    print("Healthcare Agent Chat")
    print(f"Using model: {model}")
    print("Type 'exit' or 'quit' to end the conversation.\n")

    while True:
        user_input = input("You: ").strip()

        if user_input.lower() in ["exit", "quit"]:
            print("Goodbye!")
            break

        if not user_input:
            continue

        # Add user message
        messages.append({"role": "user", "content": user_input})

        # Chat loop with function calling
        while True:
            try:
                # Make API call
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    tools=FUNCTIONS,
                    tool_choice="auto",
                )

                assistant_message = response.choices[0].message
                messages.append(assistant_message)

                # Check if function call is needed
                if assistant_message.tool_calls:
                    # Execute function calls
                    for tool_call in assistant_message.tool_calls:
                        function_name = tool_call.function.name
                        arguments = json.loads(tool_call.function.arguments)

                        print(f"\n[Tool Call: {function_name}]")
                        print(f"Arguments: {json.dumps(arguments, indent=2)}")

                        # Execute the function
                        function_result = execute_function(function_name, arguments)

                        print(f"Result: {function_result}\n")

                        # Add function result to messages
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "content": str(function_result),
                            }
                        )
                else:
                    # No function call, display response
                    print(f"\nAssistant: {assistant_message.content}\n")
                    break

            except Exception as e:
                print(f"\nError: {str(e)}\n")
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Healthcare Agent Chat - A conversational AI assistant for querying patient data.")
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o",
        help="OpenAI model to use (default: gpt-4o)",
    )
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY not found in environment variables.")
        print("Please create a .env file with your OpenAI API key.")
        print("Example: OPENAI_API_KEY=your_key_here")
        exit(1)

    chat_loop(model=args.model)

