import os
import logging
from openai import OpenAI
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataAnalyzer:
    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        logger.info(f"Initializing OpenAI client with API key: {api_key[:8]}...")  # Log only first 8 chars for security
        self.client = OpenAI(api_key=api_key)

    def analyze_table(self, table_name, schema_df, sample_data_df, schema_name):
        """Analyze table structure and sample data to generate business questions."""
        # Prepare context for GPT
        schema_description = schema_df.to_string()
        sample_data_description = sample_data_df.head().to_string()
        
        # Create prompt for GPT
        prompt = f"""Given a database table '{schema_name}.{table_name}' with the following schema:
        {schema_description}

        And sample data:
        {sample_data_description}

        Generate 5 relevant business questions that could be answered using this data.
        Format each question as a JSON object with:
        1. question_text: The business question
        2. sql_query: The SQL query to answer the question (IMPORTANT: Always use the fully qualified table name '{schema_name}.{table_name}' in the queries)
        3. visualization_type: Suggested visualization (bar, line, pie, scatter, table)
        
        Focus on questions that provide meaningful business insights.
        Make sure the SQL queries are valid and use the exact column names from the schema.
        IMPORTANT: Always prefix the table name with the schema name like this: {schema_name}.{table_name}"""

        logger.info("Sending request to OpenAI API with prompt:")
        logger.info(f"Table name: {schema_name}.{table_name}")
        logger.info(f"Schema description length: {len(schema_description)}")
        logger.info(f"Sample data description length: {len(sample_data_description)}")

        try:
            # Get response from GPT
            response = self.client.chat.completions.create(
                model="gpt-4",  # Using GPT-4
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7
            )
            
            logger.info("Received response from OpenAI API:")
            logger.info(f"Response status: Success")
            logger.info(f"Model used: {response.model}")
            logger.info(f"Response length: {len(response.choices[0].message.content)}")
            logger.info(f"Completion tokens: {response.usage.completion_tokens}")
            logger.info(f"Prompt tokens: {response.usage.prompt_tokens}")
            logger.info(f"Total tokens: {response.usage.total_tokens}")
            
            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"Error in OpenAI API call:")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error message: {str(e)}")
            if hasattr(e, 'response'):
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise Exception(f"OpenAI API error: {str(e)}")

    def generate_column_summary(self, column_data):
        """Generate a summary of the column data types and basic statistics."""
        if pd.api.types.is_numeric_dtype(column_data):
            return {
                'type': str(column_data.dtype),
                'min': column_data.min(),
                'max': column_data.max(),
                'mean': column_data.mean() if not pd.api.types.is_integer_dtype(column_data) else None,
                'null_count': column_data.isnull().sum()
            }
        else:
            return {
                'type': str(column_data.dtype),
                'unique_values': column_data.nunique(),
                'null_count': column_data.isnull().sum(),
                'sample_values': column_data.unique()[:5].tolist()
            } 