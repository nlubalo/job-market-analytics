import os
from dotenv import load_dotenv

load_dotenv()


def _get_secret(key: str, scope: str = "job-market") -> str:
    """
    On Databricks: reads from Secrets vault via dbutils.
    Locally: falls back to environment variables / .env file.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import]
        from pyspark.sql import SparkSession
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        value = os.getenv(key)
        if not value:
            raise EnvironmentError(
                f"Secret '{key}' not found in Databricks secrets or environment variables. "
                f"Set it in .env locally or run: databricks secrets put-secret {scope} {key}"
            )
        return value


ADZUNA_APP_ID = _get_secret("ADZUNA_APP_ID")
ADZUNA_APP_KEY = _get_secret("ADZUNA_APP_KEY")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

DBT_CATALOG = os.getenv("DBT_CATALOG", "job_market")

# Adzuna API
ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api"
DEFAULT_COUNTRY = "gb"
RESULTS_PER_PAGE = 50
MAX_PAGES = 1  # 1000 jobs per run; raise for full pulls
