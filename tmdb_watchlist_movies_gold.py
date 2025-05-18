from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

project_id = Variable.get("PROJECT_ID")

query_sql = f"""
    CREATE OR REPLACE TABLE `{project_id}.cinema_gold.dim_filmes_assistir` AS
    SELECT 
        id,
        title AS titulo,
        last_date
    FROM `{project_id}.cinema_silver.tmdb_watchlist_movies`;
"""

with DAG(
    'tmdb_watchlist_movies_gold',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'gold', 'movies', 'watchlist']
) as dag:

    create_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_watchlist_movies_gold',
        configuration={
            "query": {
                "query": query_sql,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )
