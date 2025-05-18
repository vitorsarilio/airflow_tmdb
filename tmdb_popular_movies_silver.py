from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'tmdb_popular_movies_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver', 'movies']
) as dag:

    create_favorites_movies_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_favorites_movies_silver',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `engestudo.cinema_silver.tmdb_popular_movies`
                    PARTITION BY DATE(job_date)
                    AS
                    SELECT 
                        id,
                        title AS titulo,
                        popularity AS popularidade,
                        job_date
                    FROM `engestudo.cinema_bronze.tmdb_popular_movies`
                    WHERE job_date IS NOT NULL
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    trigger_tmdb_popular_movies_gold = TriggerDagRunOperator(
        task_id='trigger_tmdb_popular_movies_gold',
        trigger_dag_id='tmdb_popular_movies_gold',
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True
    )

    create_favorites_movies_silver_table >> trigger_tmdb_popular_movies_gold