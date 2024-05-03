import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from urllib.parse import quote
import csv

def check_dup_add(df, row_to_add, cols_to_exclude, table_name):
    # Convert the row_to_add dictionary to a DataFrame and then concatenate it with df
    row_df = pd.DataFrame([row_to_add])
    temp_df = pd.concat([df, row_df], ignore_index=True)

    # Create a subset of columns to check by excluding the columns to exclude
    cols_to_check = [col for col in temp_df.columns if col not in cols_to_exclude]

    # Check for duplicates in columns to check
    if temp_df.duplicated(subset=cols_to_check, keep=False).iloc[-1]:  # Check the last row specifically
        return False

    # Create query to insert this new row
    columns = ', '.join([f'`{col}`' for col in row_to_add.keys()])
    values = ', '.join([f"'{val}'" if isinstance(val, str) else str(val) for val in row_to_add.values()])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    return insert_query


# Define the CSV file path
csv_file_path = 'rows_to_add.csv'

# MySQL Creds
host = ""
username = ""
password = "@"
database_name = ""
table_name = ""

# SQL query to select data from a table
sql_query = f"SELECT * FROM {table_name}"

# Columns that doesn't want to be checked for duplicates
cols_to_exclude = ['age', 'email', 'id']

# Create SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{quote(username)}:{quote(password)}@{quote(host)}/{quote(database_name)}?charset=utf8mb4")

# Usage of the function
try:
    # Execute SQL query and fetch data into a DataFrame
    df = pd.read_sql(sql_query, engine)
    
    with engine.connect() as conn:
        
        # Reading data from the CSV file
        with open(csv_file_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            
            # Iterate over each row
            for row_to_add in reader:
                insert_query = check_dup_add(df, row_to_add, cols_to_exclude, 'users')
                if insert_query != False:
                    try:
                        # make the changes, will not affect the table unless committed
                        conn.execute(text(insert_query))
                        print("Row added successfully!")
                    except Exception as e:
                        print("Error executing insert query:", e)
                else: 
                    print("Row exists, skipping this row: " + str(row_to_add))
   
        # commiting the changes
        conn.commit()
        
except Exception as e:
    print("Error:", e)
