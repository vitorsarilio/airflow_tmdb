from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import StringIO
import os
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 21),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def safe_get(data, key, default=None):
    """Função auxiliar para evitar erros com campos None"""
    if data is None:
        return default
    return data.get(key, default)

def process_movie(movie_id, headers, base_url, current_date):
    """Função para processar um único filme"""
    try:
        # Faz a requisição para a API
        url = base_url.format(ID=movie_id)
        response = requests.get(url, headers=headers, timeout=10)
        
        # Verifica se a resposta foi bem-sucedida
        if response.status_code == 429:
            print(f"Rate limit atingido para ID {movie_id}, aguardando 10 segundos...")
            sleep(10)
            response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 404:
            print(f"Filme com ID {movie_id} não encontrado (404)")
            return None, movie_id
        
        response.raise_for_status()
        
        # Verifica se a resposta contém dados válidos
        movie_data = response.json()
        if not movie_data or 'id' not in movie_data:
            print(f"Dados inválidos para o ID {movie_id}")
            return None, movie_id
        
        # Extrai os campos necessários
        movie_details = {
            'id': movie_data.get('id'),
            'backdrop_path': safe_get(movie_data, 'backdrop_path'),
            'id_collection': safe_get(movie_data.get('belongs_to_collection'), 'id'),
            'name_collection': safe_get(movie_data.get('belongs_to_collection'), 'name'),
            'poster_path_collection': safe_get(movie_data.get('belongs_to_collection'), 'poster_path'),
            'backdrop_path_collection': safe_get(movie_data.get('belongs_to_collection'), 'backdrop_path'),
            'budget': safe_get(movie_data, 'budget', 0),
            'genres_name': ', '.join([g['name'] for g in movie_data.get('genres', [])]) if movie_data.get('genres') else None,
            'homepage': safe_get(movie_data, 'homepage'),
            'imdb_id': safe_get(movie_data, 'imdb_id'),
            'original_language': safe_get(movie_data, 'original_language'),
            'original_title': safe_get(movie_data, 'original_title'),
            'overview': safe_get(movie_data, 'overview'),
            'popularity': safe_get(movie_data, 'popularity', 0.0),
            'poster_path': safe_get(movie_data, 'poster_path'),
            'release_date': safe_get(movie_data, 'release_date'),
            'revenue': safe_get(movie_data, 'revenue', 0),
            'runtime': safe_get(movie_data, 'runtime', 0),
            'status': safe_get(movie_data, 'status'),
            'title': safe_get(movie_data, 'title'),
            'vote_average': safe_get(movie_data, 'vote_average', 0.0),
            'vote_count': safe_get(movie_data, 'vote_count', 0),
            'job_date': current_date
        }
        
        print(f"Processado ID {movie_id}")
        return movie_details, None
        
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para ID {movie_id}: {str(e)}")
        return None, movie_id
    except Exception as e:
        print(f"Erro ao processar ID {movie_id}: {str(e)}")
        return None, movie_id

def fetch_movie_details(**context):
    # Configurações
    api_token = Variable.get("TMDB_API_TOKEN")
    processing_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    current_date = datetime.now(timezone.utc)
    base_url = "https://api.themoviedb.org/3/movie/{ID}?language=pt-BR"
    
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    try:
        # 1. Carrega o arquivo histórico de IDs
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket_name = 'cinema-data-lake'
        ids_history_path = "tmdb/bronze_local/movies_ids/movies_ids_history.csv"
        
        # Download e leitura do arquivo histórico
        file_bytes = gcs_hook.download(bucket_name, ids_history_path)
        file_content = file_bytes.decode('utf-8-sig') if isinstance(file_bytes, bytes) else file_bytes
        ids_df = pd.read_csv(StringIO(file_content), sep=';', dtype={'id': int})
        
        # Pega apenas IDs únicos
        movie_ids = ids_df['id'].unique()
        print(f"Total de IDs de filmes para processar: {len(movie_ids)}")

        # 2. Processa os filmes com ThreadPoolExecutor para paralelização controlada
        all_movies_details = []
        failed_ids = []
        
        # Configuração do paralelismo (4 threads para ficar dentro do rate limit)
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            # Envia todas as requisições
            for movie_id in movie_ids:
                futures.append(executor.submit(
                    process_movie, 
                    movie_id, 
                    headers, 
                    base_url, 
                    current_date
                ))
                # Pequena pausa entre requisições para evitar rate limit
                sleep(0.1)
            
            # Processa os resultados conforme ficam prontos
            for future in as_completed(futures):
                result, failed_id = future.result()
                if result:
                    all_movies_details.append(result)
                if failed_id:
                    failed_ids.append(failed_id)

        # 3. Cria DataFrame com os resultados
        details_df = pd.DataFrame(all_movies_details)
        
        # Verifica se há dados antes de salvar
        if details_df.empty:
            print("Nenhum dado válido foi processado.")
            return
        
        # 4. Gera o CSV
        csv_content = details_df.to_csv(
            sep=';',
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )

        # 5. Upload para o GCS
        details_gcs_path = f"tmdb/bronze_local/movies_details/movies_details_{processing_date}.csv"
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=details_gcs_path,
            data=csv_content,
            mime_type='text/csv; charset=utf-8'
        )
        print(f"Arquivo de detalhes salvo em: {details_gcs_path}")

        # 6. Salva lista de IDs que falharam (se houver)
        if failed_ids:
            failed_ids_path = f"tmdb/bronze_local/movies_details/failed_ids_{processing_date}.txt"
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=failed_ids_path,
                data='\n'.join(map(str, failed_ids)),
                mime_type='text/plain'
            )
            print(f"IDs com falha salvos em: {failed_ids_path}")

    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
        raise

with DAG(
    'tmdb_movies_details',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Executa às 5h
    catchup=False,
    tags=['tmdb', 'silver']
) as dag:

    fetch_details_task = PythonOperator(
        task_id='fetch_movie_details',
        python_callable=fetch_movie_details,
        provide_context=True
    )