from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator  # Correto
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'tmdb_silver_favorites_movies',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver']
) as dag:

    create_silver_table = BigQueryInsertJobOperator(
        task_id='create_silver_table',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `engestudo.cinema_silver.tmdb_favorites_movies`
                PARTITION BY DATE_TRUNC(last_date, MONTH)
                AS
                SELECT 
                  id,
                  title,
                  MIN(job_date) AS add_date,
                  MAX(job_date) AS last_date
                FROM `engestudo.cinema_bronze.tmdb_favorites_movies`
                GROUP BY id, title;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )