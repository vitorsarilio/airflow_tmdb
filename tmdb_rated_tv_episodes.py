from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def extract_tmdb_rated_tv_episodes(**context):
    # Configurações
    api_token = Variable.get("TMDB_API_TOKEN")
    account_id = "21762355"
    processing_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    base_url = f"https://api.themoviedb.org/3/account/{account_id}/rated/tv/episodes"
    
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
        # Extração de dados
        all_tv_shows_episodes = []
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        total_pages = data.get("total_pages", 1)

        for page in range(1, total_pages + 1):
            params["page"] = page
            response = requests.get(base_url, headers=headers, params=params)
            data = response.json()
            all_tv_shows_episodes.extend(data.get("results", []))

        # Transformação
        df = pd.DataFrame(all_tv_shows_episodes)[[
            "id", "show_id", "name", "overview","runtime", 
            "episode_number", "episode_type", "season_number", 
            "vote_average", "vote_count", "rating",
            "still_path","air_date"
        ]]
        df['data_hora_execucao'] = datetime.now(timezone.utc)
        df['air_date'] = pd.to_datetime(df['air_date'], errors='coerce')

        # Geração do CSV
        csv_content = df.to_csv(
            sep=';',
            index=False,
            encoding='utf-8-sig',  # Suporte a acentos e caracteres especiais
            date_format='%Y-%m-%d'
        )

        # Upload para GCS (estrutura simplificada)
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_path = f"tmdb/bronze_local/rated_tv_episodes/rated_tv_episodes_{processing_date}.csv"
        gcs_hook.upload(
            bucket_name='cinema-data-lake',
            object_name=gcs_path,
            data=csv_content,
            mime_type='text/csv; charset=utf-8'
        )

    except Exception as e:
        print(f"Erro na extração: {str(e)}")
        raise

with DAG(
    'tmdb_rated_tv_episodes',
    default_args=default_args,
    schedule_interval='35 4 * * *',
    catchup=False,
    tags=['tmdb', 'bronze']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_tmdb_rated_tv_episodes',
        python_callable=extract_tmdb_rated_tv_episodes,
        provide_context=True
    )