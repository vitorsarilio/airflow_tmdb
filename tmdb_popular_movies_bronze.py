from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from io import StringIO
import os
from time import sleep

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def extract_tmdb_popular_movies(**context):
    api_token = Variable.get("TMDB_API_TOKEN")
    processing_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    current_date = datetime.now(timezone.utc)
    current_date_naive = current_date.replace(tzinfo=None)

    base_url = "https://api.themoviedb.org/3/movie/popular"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    all_movies = []

    try:
        # Extração das 5 primeiras páginas
        for page in range(1, 6):
            params = {
                "language": "pt-BR",
                "region": "BR",
                "page": page
            }

            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            all_movies.extend(data.get("results", []))
            sleep(1)

        # Transformação
        df = pd.DataFrame(all_movies)[["id", "original_title", "title"]].drop_duplicates()
        df['job_date'] = current_date_naive

        # Inicialização do GCS
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket_name = 'cinema-data-lake'

        # Caminhos dos arquivos
        daily_gcs_path = f"tmdb/bronze_local/popular_movies/popular_movies_{processing_date}.csv"
        ids_history_path = "tmdb/bronze_local/movies_ids/movies_ids_history.csv"

        # Histórico de IDs
        existing_ids = pd.DataFrame(columns=['id', 'insertion_date', 'source_types'])
        updated_ids = pd.DataFrame(columns=['id', 'insertion_date', 'source_types'])

        try:
            file_bytes = gcs_hook.download(bucket_name, ids_history_path)
            file_content = file_bytes.decode('utf-8-sig') if isinstance(file_bytes, bytes) else file_bytes
            existing_ids = pd.read_csv(StringIO(file_content), sep=';', dtype={'id': int}, parse_dates=['insertion_date'])
            print(f"Carregados {len(existing_ids)} IDs existentes")
            updated_ids = existing_ids.copy()

            if not updated_ids.empty and pd.api.types.is_datetime64_any_dtype(updated_ids['insertion_date']):
                updated_ids['insertion_date'] = updated_ids['insertion_date'].apply(
                    lambda x: x.replace(tzinfo=None) if hasattr(x, 'tzinfo') else x
                )
        except Exception as e:
            print(f"Arquivo histórico não encontrado ou erro na leitura: {str(e)}")
            updated_ids = pd.DataFrame(columns=['id', 'insertion_date', 'source_types'])

        # Atualização de IDs
        new_ids = df[['id']].drop_duplicates()
        print(f"Encontrados {len(new_ids)} novos IDs")

        for movie_id in new_ids['id']:
            mask = updated_ids['id'] == movie_id

            if not updated_ids.empty and any(mask):
                sources = set(updated_ids.loc[mask, 'source_types'].iloc[0].split(','))
                sources.add('popular')
                updated_ids.loc[mask, 'source_types'] = ','.join(sorted(sources))

                existing_date = updated_ids.loc[mask, 'insertion_date'].iloc[0]
                if pd.notnull(existing_date):
                    existing_date_naive = existing_date.replace(tzinfo=None) if hasattr(existing_date, 'tzinfo') else existing_date
                    updated_ids.loc[mask, 'insertion_date'] = min(existing_date_naive, current_date_naive)
            else:
                new_record = pd.DataFrame([{
                    'id': movie_id,
                    'insertion_date': current_date_naive,
                    'source_types': 'popular'
                }])
                updated_ids = pd.concat([updated_ids, new_record], ignore_index=True)

        print(f"Total de IDs após atualização: {len(updated_ids)}")

        # Geração dos arquivos
        csv_content = df.to_csv(sep=';', index=False, encoding='utf-8-sig', date_format='%Y-%m-%d')

        buffer = StringIO()
        updated_ids.to_csv(buffer, sep=';', index=False, encoding='utf-8-sig', date_format='%Y-%m-%d')
        ids_csv_content = buffer.getvalue()

        # Upload para GCS
        try:
            gcs_hook.upload(bucket_name=bucket_name, object_name=daily_gcs_path, data=csv_content, mime_type='text/csv; charset=utf-8')
            gcs_hook.upload(bucket_name=bucket_name, object_name=ids_history_path, data=ids_csv_content, mime_type='text/csv; charset=utf-8')
            print(f"Arquivos enviados com sucesso: {daily_gcs_path}, {ids_history_path}")
        except Exception as upload_error:
            print(f"Erro no upload: {str(upload_error)}")
            backup_dir = "/tmp/tmdb_backups"
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            try:
                df.to_csv(f"{backup_dir}/popular_movies_{timestamp}.csv", sep=';', index=False, encoding='utf-8-sig')
                updated_ids.to_csv(f"{backup_dir}/movies_ids_history_{timestamp}.csv", sep=';', index=False, encoding='utf-8-sig')
                print(f"Backups criados em {backup_dir}")
            except Exception as backup_error:
                print(f"Erro criando backups: {str(backup_error)}")

            raise

    except Exception as e:
        print(f"Erro na extração: {str(e)}")
        raise

with DAG(
    'tmdb_popular_movies_bronze',
    default_args=default_args,
    schedule_interval='0 5 * * *',
    catchup=False,
    tags=['tmdb', 'bronze']
) as dag:

    extract_popular_movies_bronze_task = PythonOperator(
        task_id='extract_tmdb_popular_movies_bronze',
        python_callable=extract_tmdb_popular_movies,
        provide_context=True
    )
'''
    trigger_create_popular_movies_silver = TriggerDagRunOperator(
        task_id='trigger_create_popular_movies_silver_processing',
        trigger_dag_id="tmdb_popular_movies_silver",
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True
    )

    extract_popular_movies_bronze_task >> trigger_create_popular_movies_silver
'''