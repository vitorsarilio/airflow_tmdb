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
    'tmdb_rated_movies_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver']
) as dag:

    create_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_rated_movies_silver',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `engestudo.cinema_silver.tmdb_rated_movies` 
                PARTITION BY DATE_TRUNC(rated_date,MONTH) AS
                WITH last_rated_movies AS 
                (
                    SELECT  
                    trm.id,
                    trm.original_title,
                    trm.title,
                    trm.rating,
                    MIN(trm.job_date) rated_date
                    FROM 
                    `engestudo.cinema_bronze.tmdb_rated_movies` trm
                    GROUP BY
                    trm.id,
                    trm.original_title,
                    trm.title,
                    trm.rating
                ),
                rated_movies AS
                (
                    SELECT 
                    lrm.*, 
                    ROW_NUMBER() OVER (PARTITION BY lrm.id ORDER BY lrm.rated_date DESC) n_linha
                    FROM last_rated_movies lrm
                )
                SELECT
                    rm.id,
                    rm.original_title,
                    rm.title,
                    rm.rating,
                    rm.rated_date
                FROM rated_movies rm WHERE rm.n_linha = 1;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )