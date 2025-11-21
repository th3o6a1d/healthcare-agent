import os
import sqlite3
import csv

# Define the folder containing the CSV files and the SQLite database file
csv_folder = './csvs'
sqlite_db = 'synthea_data.db'

# Connect to the SQLite database (it will be created if it doesn't exist)
conn = sqlite3.connect(sqlite_db)
cursor = conn.cursor()

# Iterate over all CSV files in the folder
for file_name in os.listdir(csv_folder):
    if file_name.endswith('.csv'):
        table_name = file_name.replace('.csv', '')  # Use the file name (without .csv) as the table name
        file_path = os.path.join(csv_folder, file_name)
        
        # Read CSV file and load into SQLite
        with open(file_path, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)  # Get column names from first row
            
            # Drop existing table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table with columns from CSV headers
            # We'll use TEXT for all columns initially (SQLite is flexible with types)
            columns = ", ".join([f"{header} TEXT" for header in headers])
            cursor.execute(f"CREATE TABLE {table_name} ({columns})")
            
            # Insert data
            placeholders = ", ".join(["?" for _ in headers])
            insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"
            cursor.executemany(insert_query, csv_reader)
        
        conn.commit()
        print(f"Loaded {file_name} into table {table_name}")

# Close the database connection
conn.close()
print(f"All CSV files have been loaded into {sqlite_db}")


