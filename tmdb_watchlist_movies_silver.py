from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator  # Correto
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'tmdb_watchlist_movies_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver']
) as dag:

    create_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_watchlist_movies_silver',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `engestudo.cinema_silver.tmdb_watchlist_movies` 
                PARTITION BY DATE_TRUNC(last_date,MONTH) AS
                WITH x AS(
                    SELECT 
                    wm.id,
                    wm.title,
                    wm.job_date,
                    ROW_NUMBER() OVER(PARTITION BY wm.id ORDER BY wm.job_date DESC) n_linha
                    FROM 
                    `engestudo.cinema_bronze.tmdb_watchlist_movies` wm
                )
                SELECT 
                    x.id, 
                    x.title, 
                    x.job_date AS last_date 
                FROM x 
                    WHERE x.n_linha = 1;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )