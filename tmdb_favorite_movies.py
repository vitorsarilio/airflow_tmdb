from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import StringIO

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def extract_tmdb_favorites_movies(**context):
    # Configurações
    api_token = Variable.get("TMDB_API_TOKEN")
    account_id = "21762355"
    processing_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    current_date = datetime.now(timezone.utc)
    base_url = f"https://api.themoviedb.org/3/account/{account_id}/favorite/movies"
    
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

        # Transformação
        df = pd.DataFrame(all_movies)[["id", "original_title", "title"]]
        df['job_date'] = current_date

        # 1. Processamento do arquivo de IDs histórico compartilhado
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket_name = 'cinema-data-lake'
        ids_history_path = "tmdb/bronze_local/movies_ids/movies_ids_history.csv"
        
        # Verifica se o arquivo histórico existe
        existing_ids = pd.DataFrame(columns=['id', 'insertion_date', 'source_types'])
        
        try:
            # Tenta baixar o arquivo existente
            file_content = gcs_hook.download(
                bucket_name=bucket_name,
                object_name=ids_history_path
            )
            existing_ids = pd.read_csv(
                StringIO(file_content),
                sep=';',
                dtype={'id': int}
            )
        except Exception as e:
            print(f"Arquivo histórico não encontrado, criando novo: {str(e)}")
            # Se o arquivo não existir, continua com DataFrame vazio

        # 2. Processa os novos IDs
        new_ids = df[['id']].drop_duplicates()
        updated_ids = existing_ids.copy()
        
        # Adiciona/atualiza os tipos de fonte
        for movie_id in new_ids['id']:
            if movie_id in existing_ids['id'].values:
                # Atualiza registro existente
                mask = updated_ids['id'] == movie_id
                sources = set(updated_ids.loc[mask, 'source_types'].iloc[0].split(','))
                sources.add('favorites')
                updated_ids.loc[mask, 'source_types'] = ','.join(sorted(sources))
                # Mantém a data de inserção mais antiga
                updated_ids.loc[mask, 'insertion_date'] = min(
                    existing_ids.loc[mask, 'insertion_date'].iloc[0],
                    current_date
                )
            else:
                # Adiciona novo registro
                updated_ids = pd.concat([
                    updated_ids,
                    pd.DataFrame([{
                        'id': movie_id,
                        'insertion_date': current_date,
                        'source_types': 'favorites'
                    }])
                ], ignore_index=True)

        # 3. Geração dos CSVs
        # Arquivo completo diário
        csv_content = df.to_csv(
            sep=';',
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )

        # Arquivo histórico de IDs
        ids_csv_content = updated_ids.to_csv(
            sep=';',
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )

        # Upload para GCS
        # Arquivo diário completo
        daily_gcs_path = f"tmdb/bronze_local/favorites_movies/favorite_movies_{processing_date}.csv"
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=daily_gcs_path,
            data=csv_content,
            mime_type='text/csv; charset=utf-8'
        )

        # Arquivo histórico de IDs compartilhado
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=ids_history_path,
            data=ids_csv_content,
            mime_type='text/csv; charset=utf-8'
        )

        print(f"Total de IDs no histórico: {len(updated_ids)}")
        print(f"IDs de favorites atualizados: {len(new_ids)}")

    except Exception as e:
        print(f"Erro na extração: {str(e)}")
        raise

with DAG(
    'tmdb_favorites_movies',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['tmdb', 'bronze']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_tmdb_favorites_movies',
        python_callable=extract_tmdb_favorites_movies,
        provide_context=True
    )