import streamlit as st
import json
import plotly.express as px
import plotly.graph_objects as go
import logging
from snowflake_utils import SnowflakeConnector
from data_analyzer import DataAnalyzer

# Configure logging to capture logs in a string
import io
log_stream = io.StringIO()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=log_stream
)

st.set_page_config(page_title="Snowflake Data Explorer", layout="wide")

# Initialize session state
if 'snowflake' not in st.session_state:
    st.session_state.snowflake = None
if 'analyzer' not in st.session_state:
    st.session_state.analyzer = DataAnalyzer()
if 'schemas' not in st.session_state:
    st.session_state.schemas = None
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None
if 'tables' not in st.session_state:
    st.session_state.tables = None
if 'logs' not in st.session_state:
    st.session_state.logs = []

def initialize_connection():
    try:
        st.session_state.snowflake = SnowflakeConnector()
        return True
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return False

def create_visualization(data, viz_type, question):
    try:
        # Convert data types if needed
        numeric_cols = data.select_dtypes(include=['int64', 'float64']).columns
        categorical_cols = data.select_dtypes(include=['object', 'string', 'category']).columns
        
        if viz_type.lower() == 'bar':
            if len(categorical_cols) > 0 and len(numeric_cols) > 0:
                fig = px.bar(data, x=categorical_cols[0], y=numeric_cols[0])
            else:
                st.warning("Bar chart requires at least one categorical and one numeric column")
                return st.table(data)
                
        elif viz_type.lower() == 'line':
            if len(numeric_cols) >= 2:
                fig = px.line(data, x=numeric_cols[0], y=numeric_cols[1])
            elif len(categorical_cols) > 0 and len(numeric_cols) > 0:
                fig = px.line(data, x=categorical_cols[0], y=numeric_cols[0])
            else:
                st.warning("Line chart requires at least two numeric columns or one categorical and one numeric column")
                return st.table(data)
                
        elif viz_type.lower() == 'pie':
            if len(categorical_cols) > 0 and len(numeric_cols) > 0:
                fig = px.pie(data, names=categorical_cols[0], values=numeric_cols[0])
            else:
                st.warning("Pie chart requires at least one categorical and one numeric column")
                return st.table(data)
                
        elif viz_type.lower() == 'scatter':
            if len(numeric_cols) >= 2:
                fig = px.scatter(data, x=numeric_cols[0], y=numeric_cols[1])
            else:
                st.warning("Scatter plot requires at least two numeric columns")
                return st.table(data)
        else:
            return st.table(data)
        
        fig.update_layout(
            title=question,
            title_x=0.5,  # Center the title
            margin=dict(t=50, l=50, r=50, b=50)  # Add some margin
        )
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Failed to create visualization: {str(e)}")
        st.write("Falling back to table view:")
        st.table(data)

def main():
    st.title("Snowflake Data Explorer")
    
    # Connection status
    if st.session_state.snowflake is None:
        if st.button("Connect to Snowflake"):
            if initialize_connection():
                st.success("Connected to Snowflake!")
                # Get initial schemas after connection
                try:
                    st.session_state.schemas = st.session_state.snowflake.get_schemas()
                except Exception as e:
                    st.error(f"Failed to get schemas: {str(e)}")
        return

    try:
        # Create two columns - main content and logs
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Sidebar for navigation
            st.sidebar.title("Navigation")
            
            # Get schemas if not already loaded
            if st.session_state.schemas is None:
                st.session_state.schemas = st.session_state.snowflake.get_schemas()
            
            if st.session_state.schemas.empty:
                st.warning("No schemas found in the database.")
                return
                
            schema_names = st.session_state.schemas['name'].tolist()
            selected_schema = st.sidebar.selectbox(
                "Select Schema",
                schema_names,
                key='schema_selector'
            )

            if selected_schema:
                if selected_schema != st.session_state.selected_schema:
                    st.session_state.selected_schema = selected_schema
                    st.session_state.tables = st.session_state.snowflake.get_tables(selected_schema)
                
                if st.session_state.tables.empty:
                    st.warning(f"No tables found in schema '{selected_schema}'")
                    return
                    
                table_names = st.session_state.tables['name'].tolist()
                selected_table = st.sidebar.selectbox(
                    "Select Table",
                    table_names,
                    key='table_selector'
                )

                if selected_table:
                    st.header(f"Analyzing {selected_schema}.{selected_table}")
                    
                    # Get table information
                    with st.spinner("Loading table structure..."):
                        try:
                            schema_info = st.session_state.snowflake.get_table_schema(selected_schema, selected_table)
                            sample_data = st.session_state.snowflake.get_sample_data(selected_schema, selected_table)

                            # Display table structure
                            with st.expander("Table Structure"):
                                st.dataframe(schema_info)

                            # Display sample data
                            with st.expander("Sample Data"):
                                st.dataframe(sample_data)

                            # Generate and display business questions
                            if st.button("Generate Business Questions"):
                                with st.spinner("Analyzing data and generating questions..."):
                                    try:
                                        questions_json = st.session_state.analyzer.analyze_table(
                                            selected_table,
                                            schema_info,
                                            sample_data,
                                            selected_schema
                                        )
                                        questions = json.loads(questions_json)
                                        st.session_state.questions = questions
                                        
                                        # Capture logs
                                        log_contents = log_stream.getvalue()
                                        if log_contents:
                                            st.session_state.logs.append(log_contents)
                                            log_stream.truncate(0)
                                            log_stream.seek(0)
                                            
                                    except Exception as e:
                                        st.error(f"Failed to generate business questions: {str(e)}")
                                        # Capture error logs
                                        log_contents = log_stream.getvalue()
                                        if log_contents:
                                            st.session_state.logs.append(log_contents)
                                            log_stream.truncate(0)
                                            log_stream.seek(0)

                            # Display questions and execute selected ones
                            if 'questions' in st.session_state:
                                st.subheader("Business Questions")
                                for i, q in enumerate(st.session_state.questions):
                                    if st.button(f"📊 {q['question_text']}", key=f"q_{i}"):
                                        with st.spinner("Executing query..."):
                                            try:
                                                results = st.session_state.snowflake.execute_query(q['sql_query'])
                                                create_visualization(results, q['visualization_type'], q['question_text'])
                                            except Exception as e:
                                                st.error(f"Failed to execute query: {str(e)}")
                        except Exception as e:
                            st.error(f"Failed to load table information: {str(e)}")
        
        # Display logs in the second column
        with col2:
            st.subheader("Debug Logs")
            if st.session_state.logs:
                for log in st.session_state.logs:
                    st.text_area("Log Entry", log, height=200)
                if st.button("Clear Logs"):
                    st.session_state.logs = []
                    
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 