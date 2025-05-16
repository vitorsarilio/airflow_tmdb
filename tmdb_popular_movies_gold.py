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
    'tmdb_popular_movies_gold',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'gold', 'movies']
) as dag:

    create_tmdb_popular_movies_gold = BigQueryInsertJobOperator(
        task_id='create_tmdb_popular_movies_gold',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `engestudo.cinema_gold.dim_filmes_populares`
                    PARTITION BY job_date
                    AS
                    SELECT
                        id,
                        ANY_VALUE(titulo) AS titulo,
                        ANY_VALUE(popularidade) AS popularidade,
                        CURRENT_DATE() AS job_date
                    FROM `engestudo.cinema_silver.tmdb_popular_movies`
                    WHERE DATE(job_date) = CURRENT_DATE()
                    GROUP BY id
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )
