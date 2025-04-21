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
    'start_date': datetime(2025, 4, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def extract_tmdb_favorites_tv_shows(**context):
    # Configurações
    api_token = Variable.get("TMDB_API_TOKEN")
    account_id = "21762355"
    processing_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    base_url = f"https://api.themoviedb.org/3/account/{account_id}/favorite/tv"
    
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
        all_tv_shows = []
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        total_pages = data.get("total_pages", 1)

        for page in range(1, total_pages + 1):
            params["page"] = page
            response = requests.get(base_url, headers=headers, params=params)
            data = response.json()
            all_tv_shows.extend(data.get("results", []))

        # Transformação
        df = pd.DataFrame(all_tv_shows)[[
            "id", "original_language", "original_name", "overview", 
            "poster_path", "first_air_date", "name", 
            "vote_average", "vote_count"
        ]]
        df['data_hora_execucao'] = datetime.now(timezone.utc)
        df['first_air_date'] = pd.to_datetime(df['first_air_date'], errors='coerce')

        # Geração do CSV
        csv_content = df.to_csv(
            sep=';',
            index=False,
            encoding='utf-8-sig',  # Suporte a acentos e caracteres especiais
            date_format='%Y-%m-%d'
        )

        # Upload para GCS (estrutura simplificada)
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_path = f"tmdb/bronze_local/favorites_tv_shows/favorite_tv_shows_{processing_date}.csv"
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
    'tmdb_favorites_tv_shows',
    default_args=default_args,
    schedule_interval='30 5 * * *',
    catchup=False,
    tags=['tmdb', 'bronze']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_tmdb_favorites_tv_shows',
        python_callable=extract_tmdb_favorites_tv_shows,
        provide_context=True
    )