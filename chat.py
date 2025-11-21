import os
import json
import argparse
from typing import List, Dict, Any
from openai import OpenAI
from dotenv import load_dotenv

from tools.sqlite import query_db, get_db_schema

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
            "description": "Execute a read-only SQL query against the database and return the result as a string. Supports SELECT queries and CTEs (Common Table Expressions). The database is opened in read-only mode to prevent write operations.",
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
]

# Function mapping
FUNCTION_MAP = {
    "query_db": query_db,
    "get_db_schema": get_db_schema,
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


def chat_loop(model: str = "gpt-5"):
    """Main chat loop with function calling support."""
    messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": f"""
            
You are a helpful healthcare assistant that can query patient data and medical records from a SQLite database. 
You can help users understand patient information, analyze medical data, and answer questions about healthcare records. 

Your goal is to be comprehensive and accurate. 
You may also need to make inferences from the data to answer the question, such as inferring a diagnosis from a lab result or medication history.
You may need to explore multiple tables and do broad searches to find the information you need.

First, plan a systematic and heirarchical approach to the question.
Carefully consider each table in the database and determine how to extract relevant information from each table.
You may need to explore multiple tables and do broad searches to find the information you need.
Then, execute the plan and periodically update the user on your progress.

""",
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
                # Make streaming API call
                stream = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    tools=FUNCTIONS,
                    tool_choice="auto",
                    stream=True,
                )

                # Collect the full response for message history
                assistant_message = {"role": "assistant", "content": "", "tool_calls": []}
                current_tool_call = None
                print("\nAssistant: ", end="", flush=True)

                for chunk in stream:
                    delta = chunk.choices[0].delta
                    
                    # Handle content streaming
                    if delta.content:
                        print(delta.content, end="", flush=True)
                        assistant_message["content"] += delta.content
                    
                    # Handle tool calls
                    if delta.tool_calls:
                        for tool_call_delta in delta.tool_calls:
                            if tool_call_delta.index is not None:
                                # Ensure we have enough tool calls in our list
                                while len(assistant_message["tool_calls"]) <= tool_call_delta.index:
                                    assistant_message["tool_calls"].append({
                                        "id": "",
                                        "type": "function",
                                        "function": {"name": "", "arguments": ""}
                                    })
                                
                                current_tool_call = assistant_message["tool_calls"][tool_call_delta.index]
                                
                                if tool_call_delta.id:
                                    current_tool_call["id"] = tool_call_delta.id
                                if tool_call_delta.function:
                                    if tool_call_delta.function.name:
                                        current_tool_call["function"]["name"] = tool_call_delta.function.name
                                    if tool_call_delta.function.arguments:
                                        current_tool_call["function"]["arguments"] += tool_call_delta.function.arguments

                print()  # New line after streaming completes

                # Convert to OpenAI message format
                if assistant_message["tool_calls"]:
                    # Convert tool_calls to the format expected by OpenAI
                    tool_calls = []
                    for tc in assistant_message["tool_calls"]:
                        if tc["id"]:  # Only add if it has an ID (was actually called)
                            tool_calls.append({
                                "id": tc["id"],
                                "type": tc["type"],
                                "function": {
                                    "name": tc["function"]["name"],
                                    "arguments": tc["function"]["arguments"]
                                }
                            })
                    
                    if tool_calls:
                        api_message = {
                            "role": "assistant",
                            "content": assistant_message["content"] or None,
                            "tool_calls": tool_calls
                        }
                    else:
                        api_message = {
                            "role": "assistant",
                            "content": assistant_message["content"]
                        }
                else:
                    api_message = {
                        "role": "assistant",
                        "content": assistant_message["content"]
                    }

                messages.append(api_message)

                # Check if function call is needed
                if api_message.get("tool_calls"):
                    # Execute function calls
                    for tool_call in api_message["tool_calls"]:
                        function_name = tool_call["function"]["name"]
                        arguments = json.loads(tool_call["function"]["arguments"])

                        print(f"\n[Tool Call: {function_name}]")
                        print(f"Arguments: {json.dumps(arguments, indent=2)}")

                        # Execute the function
                        function_result = execute_function(function_name, arguments)

                        print(f"Result: {function_result}\n")

                        # Add function result to messages
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tool_call["id"],
                                "content": str(function_result),
                            }
                        )
                else:
                    # No function call, response already displayed via streaming
                    print()
                    break

            except Exception as e:
                print(f"\nError: {str(e)}\n")
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Agent Chat - A conversational AI assistant for querying patient data."
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-5.1",
        help="OpenAI model to use (default: gpt-5.1)",
    )
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY not found in environment variables.")
        print("Please create a .env file with your OpenAI API key.")
        print("Example: OPENAI_API_KEY=your_key_here")
        exit(1)

    chat_loop(model=args.model)
