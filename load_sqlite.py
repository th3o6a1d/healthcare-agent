import os
import sqlite3
import pandas as pd

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
        
        # Load the CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path)
        
        # Write the DataFrame to the SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Loaded {file_name} into table {table_name}")

# Close the database connection
conn.close()
print(f"All CSV files have been loaded into {sqlite_db}")


