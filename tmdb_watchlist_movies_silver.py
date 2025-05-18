from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    CREATE OR REPLACE TABLE `{project_id}.cinema_silver.tmdb_watchlist_movies` 
    PARTITION BY DATE_TRUNC(last_date, MONTH) AS
    WITH x AS (
        SELECT 
            wm.id,
            wm.title,
            wm.job_date,
            ROW_NUMBER() OVER (PARTITION BY wm.id ORDER BY wm.job_date DESC) n_linha
        FROM 
            `{project_id}.cinema_bronze.tmdb_watchlist_movies` wm
    )
    SELECT 
        x.id, 
        x.title, 
        x.job_date AS last_date 
    FROM x 
    WHERE x.n_linha = 1;
"""

with DAG(
    'tmdb_watchlist_movies_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver', 'movies', 'watchlist']
) as dag:

    create_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_watchlist_movies_silver',
        configuration={
            "query": {
                "query": query_sql,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    trigger_create_watchlist_movies_gold = TriggerDagRunOperator(
        task_id='trigger_tmdb_watchlist_movies_gold_processing',
        trigger_dag_id="tmdb_watchlist_movies_gold",
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True 
    )

    create_silver_table >> trigger_create_watchlist_movies_gold
