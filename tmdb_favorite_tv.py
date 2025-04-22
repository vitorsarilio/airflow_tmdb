from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import StringIO
import os

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def extract_tmdb_favorites_tv_shows(**context):
    # Configurações
    api_token = Variable.get("TMDB_API_TOKEN")
    account_id = "21762355"
    processing_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    current_date = datetime.now(timezone.utc)
    current_date_naive = current_date.replace(tzinfo=None)
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
        # 1. Extração de dados
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

        # 2. Transformação
        df = pd.DataFrame(all_tv_shows)[["id", "original_name", "name"]]
        df['job_date'] = current_date_naive

        # 3. Processamento do histórico de IDs (separado para TV shows)
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket_name = 'cinema-data-lake'
        
        # Caminhos dos arquivos
        daily_gcs_path = f"tmdb/bronze_local/favorites_tv_shows/favorite_tv_shows_{processing_date}.csv"
        ids_history_path = "tmdb/bronze_local/tv_shows_ids/tv_shows_ids_history.csv"
        
        # Inicializa DataFrames
        existing_ids = pd.DataFrame(columns=['id', 'insertion_date', 'source_types'])
        updated_ids = pd.DataFrame(columns=['id', 'insertion_date', 'source_types'])
        
        # Carrega histórico existente
        try:
            file_bytes = gcs_hook.download(bucket_name, ids_history_path)
            file_content = file_bytes.decode('utf-8-sig') if isinstance(file_bytes, bytes) else file_bytes
            existing_ids = pd.read_csv(
                StringIO(file_content),
                sep=';',
                dtype={'id': int},
                parse_dates=['insertion_date']
            )
            print(f"Carregados {len(existing_ids)} IDs de TV shows existentes")
            updated_ids = existing_ids.copy()
            
            # Converte timezones se necessário
            if not updated_ids.empty and pd.api.types.is_datetime64_any_dtype(updated_ids['insertion_date']):
                updated_ids['insertion_date'] = updated_ids['insertion_date'].apply(
                    lambda x: x.replace(tzinfo=None) if hasattr(x, 'tzinfo') else x
                )
        except Exception as e:
            print(f"Arquivo histórico de TV shows não encontrado ou erro na leitura: {str(e)}")
            updated_ids = pd.DataFrame(columns=['id', 'insertion_date', 'source_types'])

        # 4. Atualização dos IDs
        new_ids = df[['id']].drop_duplicates()
        print(f"Encontrados {len(new_ids)} novos IDs de TV shows")

        for tv_id in new_ids['id']:
            mask = updated_ids['id'] == tv_id
            
            if not updated_ids.empty and any(mask):
                # Atualiza registro existente
                sources = set(updated_ids.loc[mask, 'source_types'].iloc[0].split(','))
                sources.add('favorites_tv')
                updated_ids.loc[mask, 'source_types'] = ','.join(sorted(sources))
                
                existing_date = updated_ids.loc[mask, 'insertion_date'].iloc[0]
                if pd.notnull(existing_date):
                    existing_date_naive = existing_date.replace(tzinfo=None) if hasattr(existing_date, 'tzinfo') else existing_date
                    updated_ids.loc[mask, 'insertion_date'] = min(
                        existing_date_naive,
                        current_date_naive
                    )
            else:
                # Adiciona novo registro
                new_record = pd.DataFrame([{
                    'id': tv_id,
                    'insertion_date': current_date_naive,
                    'source_types': 'favorites_tv'
                }])
                updated_ids = pd.concat([updated_ids, new_record], ignore_index=True)

        print(f"Total de IDs de TV shows após atualização: {len(updated_ids)}")

        # 5. Geração dos arquivos CSV
        # Arquivo diário de favorites_tv_shows
        csv_content = df.to_csv(
            sep=';',
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )

        # Arquivo histórico de IDs de TV shows
        buffer = StringIO()
        updated_ids.to_csv(
            buffer,
            sep=';',
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )
        ids_csv_content = buffer.getvalue()

        # 6. Upload para o GCS
        try:
            # Upload do arquivo diário (favorites_tv_shows)
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=daily_gcs_path,
                data=csv_content,
                mime_type='text/csv; charset=utf-8'
            )
            print(f"Arquivo diário de TV shows salvo em: {daily_gcs_path}")

            # Upload do arquivo histórico (tv_shows_ids)
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=ids_history_path,
                data=ids_csv_content,
                mime_type='text/csv; charset=utf-8'
            )
            print(f"Histórico de IDs de TV shows atualizado: {ids_history_path}")

        except Exception as upload_error:
            print(f"Erro no upload: {str(upload_error)}")
            # Cria backups locais
            backup_dir = "/tmp/tmdb_tv_backups"
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            try:
                # Backup do arquivo diário
                df.to_csv(
                    f"{backup_dir}/favorite_tv_shows_{timestamp}.csv",
                    sep=';',
                    index=False,
                    encoding='utf-8-sig'
                )
                
                # Backup do histórico
                updated_ids.to_csv(
                    f"{backup_dir}/tv_shows_ids_history_{timestamp}.csv",
                    sep=';',
                    index=False,
                    encoding='utf-8-sig'
                )
                
                print(f"Backups de TV shows criados em {backup_dir}")
            except Exception as backup_error:
                print(f"Erro criando backups de TV shows: {str(backup_error)}")
            
            raise

    except Exception as e:
        print(f"Erro na extração de TV shows: {str(e)}")
        raise

with DAG(
    'tmdb_favorites_tv_shows',
    default_args=default_args,
    schedule_interval='05 4 * * *',
    catchup=False,
    tags=['tmdb', 'bronze', 'tv']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_tmdb_favorites_tv_shows',
        python_callable=extract_tmdb_favorites_tv_shows,
        provide_context=True
    )