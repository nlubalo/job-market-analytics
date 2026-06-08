import os
from dotenv import load_dotenv

load_dotenv()

ADZUNA_APP_ID = os.environ["ADZUNA_APP_ID"]
ADZUNA_APP_KEY = os.environ["ADZUNA_APP_KEY"]

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]

DBT_CATALOG = os.getenv("DBT_CATALOG", "job_market")

# Adzuna API
ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api"
DEFAULT_COUNTRY = "gb"
RESULTS_PER_PAGE = 50
MAX_PAGES = 1  # 1000 jobs per run; raise for full pulls
