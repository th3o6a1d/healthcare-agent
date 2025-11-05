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


def get_patient_data(
    patient_id: str,
    tables: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db_path: str = DB_PATH,
) -> str:
    """
    Retrieve data from specified tables for a patient, optionally filtered by date range.

    Args:
        patient_id (str): Unique patient identifier
        tables (List[str]): List of table names to retrieve data from
        
        Options:
        - demographics
        - medications
        - labs
        - lab_tests_only
        - imaging
        - procedures
        - conditions
        - problem_list
        - encounters
        - allergies
        - immunizations
        - careplans
        - devices
        
        start_date (Optional[str]): Start date for filtering (YYYY-MM-DD format)
        end_date (Optional[str]): End date for filtering (YYYY-MM-DD format)
        db_path (str): Path to the SQLite database file

    Returns:
        str: Formatted string containing data from all requested tables
    """
    # Validate date range if provided
    if start_date and end_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            if start_dt > end_dt:
                raise ValueError("Start date cannot be after end date")
            start_date = start_dt.strftime("%Y-%m-%d")
            end_date = end_dt.strftime("%Y-%m-%d")
        except ValueError as e:
            if "Start date cannot be after end date" in str(e):
                raise e
            else:
                raise ValueError(f"Invalid date format. Expected format: YYYY-MM-DD")
    elif start_date or end_date:
        raise ValueError("Both start_date and end_date must be provided together")

    available_tables = {
        "demographics": {
            "query": """
                SELECT Id, BIRTHDATE, GENDER, RACE, ETHNICITY, MARITAL, ADDRESS, CITY, STATE, ZIP,
                   CAST((julianday('now') - julianday(BIRTHDATE)) / 365.25 AS INTEGER) AS AGE
                FROM patients
                WHERE Id = ?
            """,
            "title": "DEMOGRAPHICS",
            "date_filterable": False,
        },
        "medications": {
            "query": """
                SELECT START, STOP, DESCRIPTION
                FROM medications
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY START DESC
            """,
            "title": "MEDICATIONS",
            "date_filterable": True,
            "date_column": "START",
        },
        "labs": {
            "query": """
                SELECT DATE, CATEGORY, DESCRIPTION, VALUE, UNITS, TYPE
                FROM observations
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY DATE DESC
            """,
            "title": "LABS",
            "date_filterable": True,
            "date_column": "DATE",
        },
        "lab_tests_only": {
            "query": """
                SELECT DISTINCT DESCRIPTION
                FROM observations
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY DESCRIPTION
            """,
            "title": "LAB TEST NAMES",
            "date_filterable": True,
            "date_column": "DATE",
        },
        "imaging": {
            "query": """
                SELECT DATE, BODYSITE_DESCRIPTION, MODALITY_DESCRIPTION, SOP_DESCRIPTION
                FROM imaging_studies
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY DATE DESC
            """,
            "title": "IMAGING",
            "date_filterable": True,
            "date_column": "DATE",
        },
        "procedures": {
            "query": """
                SELECT START, STOP, DESCRIPTION, CODE, SYSTEM
                FROM procedures
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY START DESC
            """,
            "title": "PROCEDURES",
            "date_filterable": True,
            "date_column": "START",
        },
        "conditions": {
            "query": """
                SELECT START, STOP, DESCRIPTION, CODE, SYSTEM
                FROM conditions
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY START DESC
            """,
            "title": "CONDITIONS",
            "date_filterable": True,
            "date_column": "START",
        },
        "problem_list": {
            "query": """
                SELECT DISTINCT DESCRIPTION
                FROM conditions
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY DESCRIPTION
            """,
            "title": "PROBLEM LIST",
            "date_filterable": True,
            "date_column": "START",
        },
        "encounters": {
            "query": """
                SELECT START, STOP, DESCRIPTION, ENCOUNTERCLASS, ORGANIZATION, PROVIDER
                FROM encounters
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY START DESC
            """,
            "title": "ENCOUNTERS",
            "date_filterable": True,
            "date_column": "START",
        },
        "allergies": {
            "query": """
                SELECT START, STOP, DESCRIPTION, TYPE, CATEGORY, DESCRIPTION1, SEVERITY1
                FROM allergies
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY START DESC
            """,
            "title": "ALLERGIES",
            "date_filterable": True,
            "date_column": "START",
        },
        "immunizations": {
            "query": """
                SELECT DATE, DESCRIPTION, CODE, BASE_COST
                FROM immunizations
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY DATE DESC
            """,
            "title": "IMMUNIZATIONS",
            "date_filterable": True,
            "date_column": "DATE",
        },
        "careplans": {
            "query": """
                SELECT START, STOP, DESCRIPTION, CODE
                FROM careplans
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY START DESC
            """,
            "title": "CARE PLANS",
            "date_filterable": True,
            "date_column": "START",
        },
        "devices": {
            "query": """
                SELECT START, STOP, DESCRIPTION, CODE, UDI
                FROM devices
                WHERE PATIENT = ?
                {date_filter}
                ORDER BY START DESC
            """,
            "title": "DEVICES",
            "date_filterable": True,
            "date_column": "START",
        },
    }

    if not tables:
        return "No tables specified. Available tables: " + ", ".join(
            available_tables.keys()
        )

    results = []

    with sqlite3.connect(db_path) as conn:
        for table in tables:
            if table not in available_tables:
                results.append(
                    f"== {table.upper()} ==\nTable '{table}' not found. Available tables: {', '.join(available_tables.keys())}"
                )
                continue

            table_config = available_tables[table]

            # Apply date filtering if requested
            date_filter = ""
            if start_date and end_date and table_config.get("date_filterable", False):
                date_column = table_config.get("date_column", "START")
                date_filter = f"AND {date_column} >= '{start_date}' AND {date_column} <= '{end_date}'"

            # Format the query with date filter
            query = table_config["query"].format(date_filter=date_filter)

            try:
                df = pd.read_sql_query(query, conn, params=(patient_id,))
                if not df.empty:
                    results.append(
                        f"== {table_config['title']} ==\n{df.to_string(index=False)}"
                    )
                else:
                    results.append(f"== {table_config['title']} ==\nNo {table} found.")
            except Exception as e:
                results.append(
                    f"== {table_config['title']} ==\nError retrieving {table}: {e}"
                )

    return "\n\n".join(results)
