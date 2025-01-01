# Snowflake Data Explorer

An interactive application that connects to Snowflake, analyzes your data, and generates business insights through AI-powered questions and visualizations.

## Features

- Connect to Snowflake databases
- Browse schemas and tables
- Analyze table structures and sample data
- AI-powered generation of relevant business questions
- Automatic SQL query generation
- Interactive data visualizations
- Easy-to-use web interface

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in your Snowflake credentials:
   ```bash
   cp .env.example .env
   ```
4. Edit `.env` with your credentials:
   - SNOWFLAKE_ACCOUNT: Your Snowflake account identifier
   - SNOWFLAKE_USER: Your username
   - SNOWFLAKE_PASSWORD: Your password
   - SNOWFLAKE_WAREHOUSE: Your warehouse name
   - SNOWFLAKE_DATABASE: Your database name
   - OPENAI_API_KEY: Your OpenAI API key

## Running the Application

Start the application with:
```bash
streamlit run app.py
```

The application will open in your default web browser.

## Usage

1. Click "Connect to Snowflake" to establish connection
2. Select a schema from the sidebar
3. Select a table to analyze
4. View table structure and sample data
5. Click "Generate Business Questions" to get AI-generated insights
6. Click on any question to execute the query and view the visualization

## Requirements

- Python 3.8+
- Snowflake account with appropriate permissions
- OpenAI API key
- Internet connection 