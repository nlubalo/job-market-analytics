# utils/db.py
# =============================================================
# Database connection and query utilities
# Manages Databricks SQL connector connection and query caching
# =============================================================

import os
import pandas as pd
import streamlit as st
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_HTTP_PATH = os.getenv('DATABRICKS_HTTP_PATH')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
CATALOG = os.getenv('DATABRICKS_CATALOG', 'job_market')
SCHEMA = os.getenv('DATABRICKS_SCHEMA', 'dev')
# dbt_project.yml sets `+schema: marts` for the marts/ layer, which dbt
# appends to the target schema (dev -> dev_marts). Dimensions/facts have
# no such override and stay in SCHEMA, so only marts need qualifying.
MARTS_SCHEMA = os.getenv('DATABRICKS_MARTS_SCHEMA', f'{SCHEMA}_marts')



def get_connection():
    """
    Returns a Databricks SQL Connection
    Called fresh per query - connection pooling handled by
    Streamlit's caching layer via @st.cache_data.
    """
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        catalog=CATALOG,
        schema=SCHEMA
    )

@st.cache_data(ttl=3600, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    """
    Executes a SQL query against Databricks and returns a DataFrame.
    Results are cached for 1 hour (ttl=3600) — adjust based on
    how frequently your dbt pipeline runs.

    Parameters
    ----------
    query : str
        SQL query to execute. Use fully qualified table names
        or rely on the catalog/schema set in the connection.

    Returns
    -------
    pd.DataFrame
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

def clear_cache():
    """
    Clears the Streamlit query cache.
    Called from the sidebar refresh button so users can
    pull fresh data without restarting the app.
    """
    run_query.clear()