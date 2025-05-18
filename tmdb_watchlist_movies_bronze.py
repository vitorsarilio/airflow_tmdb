from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def extract_tmdb_watchlist_movies(**context):
    api_token = Variable.get("TMDB_API_TOKEN")
    account_id = Variable.get("ACCOUNT_ID")
    bucket_name = Variable.get("BUCKET_NAME")
    processing_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    current_date = datetime.now(timezone.utc).replace(tzinfo=None)
    base_url = f"https://api.themoviedb.org/3/account/{account_id}/watchlist/movies"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    params = {
        "language": "pt-BR",
        "page": 1,
        "sort_by": "created_at.asc"
    }

    try:
        all_movies = []
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        total_pages = data.get("total_pages", 1)

        for page in range(1, total_pages + 1):
            params["page"] = page
            response = requests.get(base_url, headers=headers, params=params)
            data = response.json()
            all_movies.extend(data.get("results", []))

        df = pd.DataFrame(all_movies)[["id", "original_title", "title"]]
        df['job_date'] = current_date

        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        daily_gcs_path = f"tmdb/bronze_local/watchlist_movies/watchlist_movies_{processing_date}.csv"

        csv_content = df.to_csv(
            sep=';',
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )

        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=daily_gcs_path,
            data=csv_content,
            mime_type='text/csv; charset=utf-8'
        )
        print(f"Arquivo diário salvo em: {daily_gcs_path}")

    except Exception as e:
        print(f"Erro na extração: {str(e)}")
        raise

with DAG(
    'tmdb_watchlist_movies_bronze',
    default_args=default_args,
    schedule_interval='25 4 * * *',
    catchup=False,
    tags=['tmdb', 'bronze', 'movies', 'watchlist']
) as dag:

    extract_watchlist_movies_bronze_task = PythonOperator(
        task_id='extract_watchlist_movies_bronze',
        python_callable=extract_tmdb_watchlist_movies,
        provide_context=True
    )

    trigger_create_watchlist_movies_silver = TriggerDagRunOperator(
        task_id='trigger_tmdb_watchlist_movies_silver_processing',
        trigger_dag_id="tmdb_watchlist_movies_silver",
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True 
    )

    extract_watchlist_movies_bronze_task >> trigger_create_watchlist_movies_silver
