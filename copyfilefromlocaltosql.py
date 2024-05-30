import pandas as pd
from prefect import task, flow
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.sql import text
from prefect.blocks.system import Secret

# Function to get secret from Prefect Secret block
def get_secret(name):
    return Secret.load(name).get()

# Define database connection parameters using Prefect Secret blocks
SERVER_NAME = get_secret("server-name")
DATABASE_NAME = get_secret("database-name")
USERNAME = get_secret("username")
PASSWORD = get_secret("password")
DRIVER = get_secret("driver")
SCHEMA_NAME = get_secret("schema-name")
TABLE_NAME = get_secret("table-name")

# Create connection string for connecting to the SQL Database
connection_string = (
    f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER_NAME}:1433/{DATABASE_NAME}?driver={DRIVER}"
)

@task
def read_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

@task
def create_table(file_data: pd.DataFrame, table_name: str, schema_name: str):
    try:
        engine = create_engine(connection_string)
        metadata = MetaData(schema=schema_name)
        table = Table(
            table_name, metadata,
            Column('ID', Integer),
            Column('Name', String(255)),
            schema=schema_name
        )
        metadata.create_all(engine)
        
        with engine.begin() as conn:
            delete_stmt = text(f"DELETE FROM {schema_name}.{table_name}")
            result = conn.execute(delete_stmt)
            print(f"DELETE statement executed. Rows affected: {result.rowcount}")
        
        print(f"Table named {table_name} got created in the schema {schema_name}")
        print("DataFrame Contents:")
        print(file_data)
        
        with engine.connect() as conn:
            for index, row in file_data.iterrows():
                insert_query = table.insert().values(ID=row['ID'], Name=row['Name'])
                conn.execute(insert_query)
            conn.commit()
            print(f"Data inserted successfully in table {schema_name}.{table_name}")
        
        query = f"SELECT * FROM {schema_name}.{table_name}"
        table_data = pd.read_sql_query(query, engine)
        print("Data in Table:")
        print(table_data)
        
    except Exception as e:
        print(f"Error creating table or inserting data: {e}")

@flow(name="Load Data to SQL")
def file_movement_flow(file_path: str):
    file_data = read_csv(file_path)
    create_table(file_data, TABLE_NAME, SCHEMA_NAME)

if __name__ == "__main__":
    file_movement_flow('C:\\Users\\DD2107\\Downloads\\MYFILE.csv')
