import sqlite3
import pandas as pd
from typing import List, Optional, Tuple
from datetime import datetime

DB_PATH = "./synthea_data.db"


def query_db(query: str, db_path: str = DB_PATH) -> str:
    """
    Execute a read-only SQL query against the database and return the result as a string.
    
    This function is read-only and should only be used for SELECT queries.
    
    Args:
        query (str): SQL SELECT query to execute
        db_path (str): Path to the SQLite database file
        
    Returns:
        str: Formatted string containing the query results as a table
        
    Raises:
        ValueError: If the query contains non-SELECT operations (for safety)
    """
    # Basic safety check - ensure query is read-only
    query_upper = query.strip().upper()
    if not query_upper.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed. This is a read-only function.")
    
    # Check for dangerous operations
    dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]
    for keyword in dangerous_keywords:
        if keyword in query_upper:
            raise ValueError(f"Query contains forbidden keyword: {keyword}. This is a read-only function.")
    
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(query, conn)
            return df.to_string(index=False) if not df.empty else "No results found."
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


def compare_dates(
    date1: str,
    date2: str,
    date_format: str = "%Y-%m-%d",
    db_path: str = DB_PATH,
) -> dict:
    """
    Compare two dates and return information about their relationship.
    
    Args:
        date1 (str): First date in the specified format (default: YYYY-MM-DD)
        date2 (str): Second date in the specified format (default: YYYY-MM-DD)
        date_format (str): Format of the input dates (default: "%Y-%m-%d")
        db_path (str): Path to the SQLite database file (not used, but kept for consistency)
        
    Returns:
        dict: Dictionary containing comparison results:
            - date1_earlier: bool - True if date1 is before date2
            - date2_earlier: bool - True if date2 is before date1
            - dates_equal: bool - True if dates are equal
            - difference_days: int - Number of days between dates (absolute value)
            - date1_parsed: str - date1 in YYYY-MM-DD format
            - date2_parsed: str - date2 in YYYY-MM-DD format
            
    Raises:
        ValueError: If dates are invalid or in wrong format
    """
    try:
        # Parse the input dates
        dt1 = datetime.strptime(date1, date_format)
        dt2 = datetime.strptime(date2, date_format)
        
        # Calculate difference
        difference = abs((dt2 - dt1).days)
        
        return {
            "date1_earlier": dt1 < dt2,
            "date2_earlier": dt2 < dt1,
            "dates_equal": dt1 == dt2,
            "difference_days": difference,
            "date1_parsed": dt1.strftime("%Y-%m-%d"),
            "date2_parsed": dt2.strftime("%Y-%m-%d"),
        }
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected format: {date_format}. Error: {e}")

