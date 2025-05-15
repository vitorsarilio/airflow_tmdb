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
from google.cloud import bigquery

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

def process_movie(movie_id, headers, base_url, current_date, include_job_date=True, index=None):
    try:
        url = base_url.format(ID=movie_id)
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 429:
            print(f"[429] Rate limit atingido para ID {movie_id}. Aguardando 10s...")
            sleep(10)
            response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 404:
            print(f"[404] Filme {movie_id} não encontrado.")
            return None, None

        response.raise_for_status()
        movie_data = response.json()

        if not movie_data or 'id' not in movie_data or movie_data.get('adult') == True:
            print(f"[SKIP] ID {movie_id} ignorado por conteúdo adulto ou dados inválidos.")
            return None, None

        result = {
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
            'updated_date': current_date
        }

        if include_job_date:
            result['job_date'] = current_date

        print(f"[✓] Registro processado: {index if index is not None else movie_id}")
        return result, None

    except Exception as e:
        print(f"[ERRO] Falha ao atualizar ID {movie_id}: {e}")
        return None, movie_id

def fetch_movie_details(**context):
    print("[START] Iniciando coleta de novos filmes do TMDB")

    api_token = Variable.get("TMDB_API_TOKEN")
    current_date = datetime.now(timezone.utc)
    base_url = "https://api.themoviedb.org/3/movie/{ID}?language=pt-BR"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = hook.get_client()

    query_ids = """
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

    BATCH_SIZE = 100
    for i in range(0, len(ids_novos), BATCH_SIZE):
        batch_ids = ids_novos[i:i+BATCH_SIZE]
        print(f"[LOTE {i//BATCH_SIZE + 1}] Processando {len(batch_ids)} IDs...")

        all_details = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for idx, movie_id in enumerate(batch_ids):
                futures.append(executor.submit(
                    process_movie,
                    movie_id,
                    headers,
                    base_url,
                    current_date,
                    include_job_date=True,
                    index=idx + 1 + i
                ))
                sleep(0.1)

            for future in as_completed(futures):
                result, _ = future.result()
                if result:
                    all_details.append(result)

        if all_details:
            df = pd.DataFrame(all_details)
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce').dt.date
            client.load_table_from_dataframe(df, 'engestudo.cinema_bronze.tmdb_movies_details').result()
            print(f"[✓] Lote {i//BATCH_SIZE + 1} inserido com {len(df)} registros.")

    print("[END] Coleta de novos filmes finalizada.")

def atualizar_filmes_modificados(**context):
    print("[START] Atualizando filmes modificados do TMDB")

    api_token = Variable.get("TMDB_API_TOKEN")
    current_date = datetime.now(timezone.utc)
    base_url = "https://api.themoviedb.org/3/movie/{ID}?language=pt-BR"
    changes_url = "https://api.themoviedb.org/3/movie/changes"

    start_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = current_date.strftime('%Y-%m-%d')

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_token}"
    }

    modified_ids = []
    page = 1
    while True:
        response = requests.get(f"{changes_url}?start_date={start_date}&end_date={end_date}&page={page}", headers=headers)
        response.raise_for_status()
        data = response.json()
        modified = [item['id'] for item in data.get('results', []) if not item.get('adult', False)]
        modified_ids.extend(modified)
        print(f"[INFO] Página {page}: {len(modified)} IDs válidos.")
        if page >= data.get('total_pages', 1):
            break
        page += 1
        sleep(0.1)

    print(f"[INFO] Total de filmes modificados: {len(modified_ids)}")

    if not modified_ids:
        return

    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = hook.get_client()
    BATCH_SIZE = 100

    for i in range(0, len(modified_ids), BATCH_SIZE):
        batch_ids = modified_ids[i:i+BATCH_SIZE]
        print(f"[LOTE MOD {i//BATCH_SIZE + 1}] Processando {len(batch_ids)} IDs modificados...")

        all_details = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for idx, movie_id in enumerate(batch_ids):
                futures.append(executor.submit(
                    process_movie,
                    movie_id,
                    headers,
                    base_url,
                    current_date,
                    include_job_date=False,
                    index=idx + 1 + i
                ))
                sleep(0.1)

            for future in as_completed(futures):
                result, _ = future.result()
                if result:
                    all_details.append(result)

        if all_details:
            for r in all_details:
                print(f"[✓] Filme atualizado: ID {r['id']} - {r.get('title', '')}")

            df = pd.DataFrame(all_details)
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce').dt.date
            df['id_collection'] = pd.to_numeric(df['id_collection'], errors='coerce').astype('Int64')

            temp_table = 'engestudo.cinema_bronze.tmdb_movies_details_temp'
            client.load_table_from_dataframe(df, temp_table, job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            )).result()

            merge_sql = f"""
                MERGE `engestudo.cinema_bronze.tmdb_movies_details` T
                USING `engestudo.cinema_bronze.tmdb_movies_details_temp` S
                ON T.id = S.id
                WHEN MATCHED THEN UPDATE SET
                    backdrop_path = S.backdrop_path,
                    id_collection = S.id_collection,
                    name_collection = S.name_collection,
                    poster_path_collection = S.poster_path_collection,
                    backdrop_path_collection = S.backdrop_path_collection,
                    budget = S.budget,
                    genres_name = S.genres_name,
                    homepage = S.homepage,
                    imdb_id = S.imdb_id,
                    original_language = S.original_language,
                    original_title = S.original_title,
                    overview = S.overview,
                    popularity = S.popularity,
                    poster_path = S.poster_path,
                    release_date = S.release_date,
                    revenue = S.revenue,
                    runtime = S.runtime,
                    status = S.status,
                    title = S.title,
                    vote_average = S.vote_average,
                    vote_count = S.vote_count,
                    updated_date = S.updated_date
                WHEN NOT MATCHED THEN
                INSERT (id, backdrop_path, id_collection, name_collection, poster_path_collection, backdrop_path_collection,
                        budget, genres_name, homepage, imdb_id, original_language, original_title, overview, popularity,
                        poster_path, release_date, revenue, runtime, status, title, vote_average, vote_count, updated_date, job_date)
                VALUES (S.id, S.backdrop_path, S.id_collection, S.name_collection, S.poster_path_collection, S.backdrop_path_collection,
                        S.budget, S.genres_name, S.homepage, S.imdb_id, S.original_language, S.original_title, S.overview, S.popularity,
                        S.poster_path, S.release_date, S.revenue, S.runtime, S.status, S.title, S.vote_average, S.vote_count, S.updated_date, S.updated_date)
            """
            client.query(merge_sql).result()

            delete_sql = f"DROP TABLE IF EXISTS `{temp_table}`"
            client.query(delete_sql).result()

            print(f"[✓] Lote MOD {i//BATCH_SIZE + 1} atualizado com {len(df)} registros via MERGE.")

    print("[END] Atualização de filmes modificados finalizada.")

with DAG(
    'tmdb_details_movies_bronze',
    default_args=default_args,
    schedule_interval='30 9 * * *',
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

    atualizar_modificados = PythonOperator(
        task_id='atualizar_filmes_modificados',
        python_callable=atualizar_filmes_modificados,
        provide_context=True
    )

    trigger_create_details_movies_silver = TriggerDagRunOperator(
        task_id='trigger_create_details_movies_silver_processing',
        trigger_dag_id="tmdb_details_movies_silver",
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True
    )

    criar_tabela_tmdb_movies_details >> fetch_task >> atualizar_modificados >> trigger_create_details_movies_silver
