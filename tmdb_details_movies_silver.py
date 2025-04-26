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
    'tmdb_details_movies_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver']
) as dag:

    create_details_movies_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_details_movies_silver',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `engestudo.cinema_silver.tmdb_movies_details` 
                PARTITION BY DATE_TRUNC(job_date,MONTH) AS
                WITH movies_details AS 
                (
                    SELECT  
                    tmd.id,
                    SAFE_CAST(tmd.id_collection AS INT64) AS id_collection,
                    tmd.imdb_id,
                    tmd.original_title,
                    tmd.title,
                    tmd.name_collection,
                    tmd.genres_name,
                    tmd.overview,
                    tmd.runtime,
                    tmd.budget,
                    tmd.revenue,
                    tmd.status,
                    tmd.release_date,
                    tmd.original_language,
                    tmd.vote_count,
                    tmd.vote_average,
                    tmd.popularity,
                    tmd.backdrop_path,
                    tmd.backdrop_path_collection,
                    tmd.poster_path,
                    tmd.poster_path_collection,
                    tmd.homepage,
                    tmd.job_date,
                    ROW_NUMBER() OVER (PARTITION BY tmd.id ORDER BY tmd.job_date DESC) n_linha
                    FROM 
                    `engestudo.cinema_bronze.tmdb_movies_details` tmd
                )
                SELECT
                    md.* EXCEPT(n_linha)
                FROM movies_details md WHERE md.n_linha = 1;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )