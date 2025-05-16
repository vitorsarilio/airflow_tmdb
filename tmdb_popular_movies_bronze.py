from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
import time

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(hours=1)
}

def extract_tmdb_popular_movies(**context):
    api_token = Variable.get("TMDB_API_TOKEN")
    current_date = datetime.now(timezone.utc)

    base_url = "https://api.themoviedb.org/3/movie/popular"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    all_movies = []
    for page in range(1, 55):
        print(f"[EXTRACAO] Página {page}")
        params = {"language": "pt-BR", "region": "BR", "page": page}
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        movies = response.json().get("results", [])
        all_movies.extend(movies)
        time.sleep(0.5)

    df = pd.DataFrame(all_movies)[["id", "title", "popularity"]].drop_duplicates()
    df["job_date"] = current_date
    df["index"] = range(1, len(df) + 1)  # número do registro

    print(f"[INFO] Total de registros a inserir: {len(df)}")

    for i, row in df.iterrows():
        print(f"[{row['index']}] ID: {row['id']} | Título: {row['title']}")

    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = bq_hook.get_client()

    # Tipagem obrigatória para evitar erros
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
    df["job_date"] = pd.to_datetime(df["job_date"])
    df = df.drop(columns=["index"])

    table_id = "engestudo.cinema_bronze.tmdb_popular_movies"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"[✓] Inserção concluída no BigQuery: {table_id}")

with DAG(
    dag_id='tmdb_popular_movies_bronze',
    default_args=default_args,
    schedule_interval='0 5 * * *',
    catchup=False,
    tags=['tmdb', 'bronze', 'movies']
) as dag:

    criar_tabela_tmdb_popular_movies = BigQueryInsertJobOperator(
        task_id='criar_tabela_tmdb_popular_movies',
        configuration={
            "query": {
                "query": """
                    CREATE TABLE IF NOT EXISTS `engestudo.cinema_bronze.tmdb_popular_movies`
                    PARTITION BY DATE(job_date)
                    AS SELECT
                        CAST(NULL AS INT64) AS id,
                        CAST(NULL AS STRING) AS title,
                        CAST(NULL AS FLOAT64) AS popularity,
                        CAST(NULL AS TIMESTAMP) AS job_date
                    LIMIT 0
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    extrair_e_inserir = PythonOperator(
        task_id='extrair_e_inserir_popular_movies',
        python_callable=extract_tmdb_popular_movies,
        provide_context=True
    )

    trigger_create_details_movies_silver = TriggerDagRunOperator(
        task_id='trigger_create_tmdb_popular_movies_silver_processing',
        trigger_dag_id="tmdb_popular_movies_silver",
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True
    )

    criar_tabela_tmdb_popular_movies >> extrair_e_inserir >> trigger_create_details_movies_silver
