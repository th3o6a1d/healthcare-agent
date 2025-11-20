import sqlite3
import pandas as pd
from typing import List, Optional, Tuple
from datetime import datetime

DB_PATH = "./synthea_data.db"


def query_db(query: str, db_path: str = DB_PATH) -> str:
    """
    Execute a read-only SQL query against the database and return the result as a string.
    
    This function is read-only and supports SELECT queries and CTEs (Common Table Expressions).
    The database is opened in read-only mode to prevent any write operations.
    
    Args:
        query (str): SQL SELECT query or CTE to execute
        db_path (str): Path to the SQLite database file
        
    Returns:
        str: Formatted string containing the query results as a table
        
    Raises:
        ValueError: If the query contains write operations (for safety)
    """
    # Check for dangerous write operations
    query_upper = query.strip().upper()
    # Remove comments and normalize whitespace for keyword detection
    # Split by common comment patterns and SQL statement separators
    query_normalized = " ".join(query_upper.split())
    
    # Check for dangerous write operations (but allow them in string literals would be complex,
    # so we rely on SQLite's read-only mode as the primary protection)
    dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
    for keyword in dangerous_keywords:
        # Check if keyword appears as a standalone word (not part of another word)
        # This is a simple check - SQLite's read-only mode will catch actual attempts
        if f" {keyword} " in query_normalized or query_normalized.startswith(f"{keyword} "):
            raise ValueError(f"Query contains forbidden keyword: {keyword}. This is a read-only function.")
    
    try:
        # Open database in read-only mode using URI parameter
        # This prevents any write operations at the database level
        db_uri = f"file:{db_path}?mode=ro"
        with sqlite3.connect(db_uri, uri=True) as conn:
            df = pd.read_sql_query(query, conn)
            return df.to_string(index=False) if not df.empty else "No results found."
    except sqlite3.OperationalError as e:
        if "readonly" in str(e).lower() or "database is locked" in str(e).lower():
            return f"Error: Database is read-only or locked. {e}"
        return f"Error executing query: {e}"
    except Exception as e:
        return f"Error executing query: {e}"


def get_db_schema(db_path: str = DB_PATH) -> str:
    """
    Return the schema of all tables in the database.
    
    Args:
        db_path (str): Path to the SQLite database file
        
    Returns:
        str: Formatted string containing the schema of all tables
    """
    print(f"Getting schema for database at {db_path}")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        output = []
        for (table_name,) in tables:
            output.append(f"--- Schema for table {table_name} ---")
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            for col in columns:
                cid, name, type_, notnull, dflt, pk = col
                output.append(
                    f"Column: {name} | Type: {type_} | Not Null: {notnull} | Default: {dflt} | PK: {pk}"
                )
            output.append("")
        
        return "\n".join(output) if output else "No tables found."