import pandas as pd # Import pandas library for data manipulation

from prefect import task, flow # Import task and flow decorators from Prefect

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData # Import SQLAlchemy components for database operations

from sqlalchemy.sql import text # Import text function for raw SQL queries

from prefect.blocks.system import Secret # Import Secret block for secure storage of secrets

# Define database connection parameters using secrets stored in Prefect Cloud

# Define database connection parameters
SERVER_NAME = Secret.load("server-name").get()
DATABASE_NAME = Secret.load("database-name").get()
USERNAME = Secret.load("username").get()
PASSWORD = Secret.load("password").get()
DRIVER = Secret.load("driver").get()
SCHEMA_NAME = Secret.load("schema-name").get()

# Create connection string for connecting to the Azure SQL Database

connection_string = (

f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER_NAME}:1433/{DATABASE_NAME}?driver={DRIVER}"

)

# Function to read data from a CSV file

@task

def read_csv(file_path: str) -> pd.DataFrame:

return pd.read_csv(file_path) # Read CSV file into a DataFrame

# Function to connect to Azure SQL Database and create a new table

@task

def create_table(file_data: pd.DataFrame, table_name: str, schema_name: str):

try:

# Connect to the database

engine = create_engine(connection_string) # Create a database engine

metadata = MetaData(schema=schema_name) # Define metadata with the specified schema

# Define a new table with the provided name in the specified schema

table = Table(

table_name, metadata,

Column('ID', Integer), # Define 'ID' column as Integer

Column('Name', String(255)), # Define 'Name' column as String with max length 255

schema=schema_name

)

# Create the table in the database

metadata.create_all(engine) # Create all tables defined in metadata

# Clear the table before inserting new data

with engine.begin() as conn:

delete_stmt = text(f"DELETE FROM {schema_name}.{table_name}") # Define delete statement

result = conn.execute(delete_stmt) # Execute delete statement

print(f"DELETE statement executed. Rows affected: {result.rowcount}") # Print number of rows deleted

print(f"Table named {table_name} got created in the schema {schema_name}") # Confirm table creation

# Print DataFrame contents

print("DataFrame Contents:")

print(file_data) # Display contents of the DataFrame

# Insert data into the new table

with engine.connect() as conn:

for index, row in file_data.iterrows(): # Iterate through each row in the DataFrame

insert_query = table.insert().values(ID=row['ID'], Name=row['Name']) # Define insert query for each row

conn.execute(insert_query) # Execute insert query

# Commit the transaction

conn.commit() # Commit all insert operations

print(f"Data inserted successfully in table {schema_name}.{table_name}") # Confirm data insertion

# Query data from the table

query = f"SELECT * FROM {schema_name}.{table_name}" # Define select query to fetch data

table_data = pd.read_sql_query(query, engine) # Execute select query and load data into a DataFrame

# Print the data

print("Data in Table:")

print(table_data) # Display data in the table

except Exception as e:

print(f"Error creating table or inserting data: {e}") # Print error message if any exception occurs

# Define the flow

@flow(name="Load Data to Azure SQL")

def file_movement_flow():

# Load data from CSV file

file_data = read_csv(r"C:\Users\DD2107\Downloads\MYFILE.csv") # Read CSV file and store data in file_data

# Create table and insert data in the specified schema

create_table(file_data, TABLE_NAME, SCHEMA_NAME) # Call create_table task to create the table and insert data

# Run the flow

if __name__ == "__main__":

file_movement_flow() # Execute the flow if this script is run directly
