from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import requests
import pandas as pd

# Default args
default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 21),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=24)
}

def safe_get(data, key, default=None):
    if data is None:
        return default
    return data.get(key, default)

def process_movie(movie_id, headers, base_url, current_date):
    try:
        url = base_url.format(ID=movie_id)
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 429:
            print(f"[429] Rate limit atingido para ID {movie_id}. Aguardando 10s...")
            sleep(10)
            response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 404:
            print(f"[404] Filme {movie_id} não encontrado.")
            return None, movie_id

        response.raise_for_status()
        movie_data = response.json()

        if not movie_data or 'id' not in movie_data:
            print(f"[Erro] Dados inválidos para ID {movie_id}")
            return None, movie_id

        return {
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
            'job_date': current_date,
            'updated_date': current_date
        }, None

    except requests.exceptions.RequestException as e:
        print(f"[Erro Req] ID {movie_id}: {str(e)}")
        return None, movie_id
    except Exception as e:
        print(f"[Erro Geral] ID {movie_id}: {str(e)}")
        return None, movie_id

def fetch_movie_details(**context):
    print("[START] Iniciando DAG de coleta de detalhes dos filmes TMDB")

    api_token = Variable.get("TMDB_API_TOKEN")
    current_date = datetime.now(timezone.utc)
    data_hoje = (current_date - timedelta(days=0)).strftime('%Y-%m-%d')
    base_url = "https://api.themoviedb.org/3/movie/{ID}?language=pt-BR"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = hook.get_client()

    INCREMENTO = 100_000
    query_ids = f"""
        SELECT id
        FROM `engestudo.cinema_bronze.tmdb_movies_ids_history`
        WHERE id NOT IN (
              SELECT DISTINCT id
              FROM `engestudo.cinema_bronze.tmdb_movies_details`
          )
    """

    df_ids = client.query(query_ids).to_dataframe()
    ids_novos = df_ids['id'].tolist()

    print(f"[INFO] Total de novos IDs a processar: {len(ids_novos)}")

    if not ids_novos:
        print("[✓] Nenhum novo ID encontrado para processar.")
        return

    BATCH_SIZE = 1000
    for i in range(0, len(ids_novos), BATCH_SIZE):
        batch_ids = ids_novos[i:i + BATCH_SIZE]
        print(f"[LOTE {i//BATCH_SIZE + 1}] Iniciando processamento de {len(batch_ids)} IDs...")

        all_details = []
        failed = []

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for movie_id in batch_ids:
                futures.append(executor.submit(
                    process_movie,
                    movie_id,
                    headers,
                    base_url,
                    current_date
                ))
                sleep(0.1)

            for future in as_completed(futures):
                result, failed_id = future.result()
                if result:
                    all_details.append(result)
                if failed_id:
                    failed.append(failed_id)

        if all_details:
            df = pd.DataFrame(all_details)
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce').dt.date
            client.load_table_from_dataframe(df, 'engestudo.cinema_bronze.tmdb_movies_details').result()
            print(f"[✓] Lote {i//BATCH_SIZE + 1} inserido com {len(df)} registros.")

        if failed:
            print(f"[!] {len(failed)} IDs falharam no lote {i//BATCH_SIZE + 1}: {failed[:5]}...")

        sleep(3)

    print("[END] DAG finalizada com sucesso.")

with DAG(
    'tmdb_details_movies_bronze',
    default_args=default_args,
    schedule_interval='30 8 * * *',
    catchup=False,
    tags=['tmdb', 'bronze', 'movies']
) as dag:

    criar_tabela_tmdb_movies_details = BigQueryInsertJobOperator(
        task_id='criar_tabela_tmdb_movies_details',
        configuration={
            "query": {
                "query": """
                    CREATE TABLE IF NOT EXISTS `engestudo.cinema_bronze.tmdb_movies_details`
                    PARTITION BY DATE(job_date)
                    CLUSTER BY updated_date AS
                    SELECT
                      CAST(NULL AS INT64) AS id,
                      CAST(NULL AS STRING) AS backdrop_path,
                      CAST(NULL AS INT64) AS id_collection,
                      CAST(NULL AS STRING) AS name_collection,
                      CAST(NULL AS STRING) AS poster_path_collection,
                      CAST(NULL AS STRING) AS backdrop_path_collection,
                      CAST(NULL AS INT64) AS budget,
                      CAST(NULL AS STRING) AS genres_name,
                      CAST(NULL AS STRING) AS homepage,
                      CAST(NULL AS STRING) AS imdb_id,
                      CAST(NULL AS STRING) AS original_language,
                      CAST(NULL AS STRING) AS original_title,
                      CAST(NULL AS STRING) AS overview,
                      CAST(NULL AS FLOAT64) AS popularity,
                      CAST(NULL AS STRING) AS poster_path,
                      CAST(NULL AS DATE) AS release_date,
                      CAST(NULL AS INT64) AS revenue,
                      CAST(NULL AS INT64) AS runtime,
                      CAST(NULL AS STRING) AS status,
                      CAST(NULL AS STRING) AS title,
                      CAST(NULL AS FLOAT64) AS vote_average,
                      CAST(NULL AS INT64) AS vote_count,
                      CAST(NULL AS TIMESTAMP) AS job_date,
                      CAST(NULL AS TIMESTAMP) AS updated_date
                    LIMIT 0
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    fetch_task = PythonOperator(
        task_id='fetch_tmdb_movie_details',
        python_callable=fetch_movie_details,
        provide_context=True
    )

    trigger_create_details_movies_silver = TriggerDagRunOperator(
        task_id='trigger_create_details_movies_silver_processing',
        trigger_dag_id="tmdb_details_movies_silver",
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True 
    )

    criar_tabela_tmdb_movies_details >> fetch_task >> trigger_create_details_movies_silver
