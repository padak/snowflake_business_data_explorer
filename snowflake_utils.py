import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class SnowflakeConnector:
    def __init__(self):
        # Extract account name from the full URL if needed
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        if '.snowflakecomputing.com' in account:
            account = account.replace('.snowflakecomputing.com', '')

        database = os.getenv('SNOWFLAKE_DATABASE')
        if database and database.startswith('"') and database.endswith('"'):
            database = database.strip('"')

        try:
            self.conn = snowflake.connector.connect(
                account=account,
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=database,
                insecure_mode=False
            )
            self.cur = self.conn.cursor()
            
            # Set the database context explicitly
            self.cur.execute(f'USE DATABASE "{database}"')
            # Set the warehouse context explicitly
            self.cur.execute(f'USE WAREHOUSE "{os.getenv("SNOWFLAKE_WAREHOUSE")}"')
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {str(e)}")

    def get_schemas(self):
        """Get all schemas in the current database."""
        try:
            query = """
            SELECT DISTINCT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT LIKE 'INFORMATION_SCHEMA'
            AND schema_name NOT LIKE 'PUBLIC'
            ORDER BY schema_name
            """
            df = pd.read_sql(query, self.conn)
            # Rename the column to 'name' for consistency
            df = df.rename(columns={'SCHEMA_NAME': 'name'})
            return df
        except Exception as e:
            raise Exception(f"Failed to get schemas: {str(e)}")

    def get_tables(self, schema):
        """Get all tables in the specified schema."""
        try:
            query = f"""
            SELECT DISTINCT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            df = pd.read_sql(query, self.conn)
            # Rename the column to 'name' for consistency
            df = df.rename(columns={'TABLE_NAME': 'name'})
            return df
        except Exception as e:
            raise Exception(f"Failed to get tables for schema {schema}: {str(e)}")

    def get_table_schema(self, schema, table):
        """Get column information for the specified table."""
        try:
            query = f"""
            SELECT 
                column_name as "Column",
                data_type as "Type",
                is_nullable as "Nullable",
                character_maximum_length as "Max Length"
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table}'
            ORDER BY ordinal_position
            """
            return pd.read_sql(query, self.conn)
        except Exception as e:
            raise Exception(f"Failed to get schema for table {schema}.{table}: {str(e)}")

    def get_sample_data(self, schema, table, limit=100):
        """Get sample data from the specified table."""
        try:
            query = f'SELECT * FROM "{schema}"."{table}" LIMIT {limit}'
            return pd.read_sql(query, self.conn)
        except Exception as e:
            raise Exception(f"Failed to get sample data from {schema}.{table}: {str(e)}")

    def execute_query(self, query):
        """Execute a custom query and return results as a DataFrame."""
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            raise Exception(f"Failed to execute query: {str(e)}")

    def close(self):
        """Close the Snowflake connection."""
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close() 