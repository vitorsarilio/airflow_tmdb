from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'tmdb_now_playing_movies_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver']
) as dag:

    create_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_now_playing_movies_silver',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `engestudo.cinema_silver.tmdb_now_playing_movies` 
                PARTITION BY DATE_TRUNC(last_date,MONTH) AS
                WITH x AS (
                SELECT
                    npm.id,
                    npm.original_title,
                    npm.title,
                    npm.job_date,
                    ROW_NUMBER() OVER (PARTITION BY npm.id ORDER BY npm.job_date DESC) n_linha
                FROM 
                    `engestudo.cinema_bronze.tmdb_now_playing_movies` npm
                )
                SELECT 
                x.id,
                x.original_title,
                x.title,
                x.job_date AS last_date,
                FROM x
                WHERE n_linha = 1;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )