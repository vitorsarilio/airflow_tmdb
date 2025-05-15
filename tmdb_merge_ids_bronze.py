from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow.utils.dates import days_ago
from datetime import datetime
import gzip, json, io, pandas as pd, requests

# CONFIGURAÇÕES
PROJECT_ID = 'engestudo'
DATASET = 'cinema_bronze'
TABLE = 'tmdb_movies_ids_history'
BUCKET_NAME = 'cinema-data-lake'
STAGING_FILE = 'tmdb/movies_ids_temp.csv'
TEMP_TABLE = 'movies_ids_temp'

def baixar_processar_salvar_gcs(**context):
    data_str = datetime.now().strftime('%m_%d_%Y')
    url = f'https://files.tmdb.org/p/exports/movie_ids_{data_str}.json.gz'
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro ao baixar: {url}")
    
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
        dados = [json.loads(linha) for linha in f]
    
    df = pd.DataFrame(dados)[['id']].drop_duplicates()
    df['source_type'] = 'daily_dump'
    df['data_processamento'] = datetime.now()

    # Salvar no GCS
    gcs_hook = GCSHook()
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=STAGING_FILE, data=csv_buffer.getvalue())
    print(f"Arquivo enviado para gs://{BUCKET_NAME}/{STAGING_FILE}")

# SQLs
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{TABLE}` (
    id INT64,
    source_type STRING,
    data_processamento TIMESTAMP
)
"""

MERGE_SQL = f"""
MERGE `{PROJECT_ID}.{DATASET}.{TABLE}` T
USING `{PROJECT_ID}.{DATASET}.{TEMP_TABLE}` S
ON T.id = S.id
WHEN NOT MATCHED THEN
  INSERT (id, source_type, data_processamento)
  VALUES (S.id, S.source_type, S.data_processamento)
"""

DROP_TEMP_SQL = f"""
DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET}.{TEMP_TABLE}`
"""

# DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='tmdb_merge_ids_bronze',
    default_args=default_args,
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['tmdb', 'bigquery'],
) as dag:

    criar_tabela_if_needed = BigQueryInsertJobOperator(
        task_id='criar_tabela_movies_ids_history',
        configuration={
            "query": {
                "query": CREATE_TABLE_SQL,
                "useLegacySql": False
            }
        }
    )

    baixar_e_subir_gcs = PythonOperator(
        task_id='baixar_e_salvar_gcs',
        python_callable=baixar_processar_salvar_gcs
    )

    carregar_temp_table = BigQueryInsertJobOperator(
        task_id='carregar_temp_table',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/{STAGING_FILE}"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET,
                    "tableId": TEMP_TABLE,
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True
            }
        }
    )

    merge_para_tabela_principal = BigQueryInsertJobOperator(
        task_id='executar_merge',
        configuration={
            "query": {
                "query": MERGE_SQL,
                "useLegacySql": False
            }
        }
    )

    drop_temp_table = BigQueryInsertJobOperator(
        task_id='remover_temp_table',
        configuration={
            "query": {
                "query": DROP_TEMP_SQL,
                "useLegacySql": False
            }
        }
    )

    # Dependências
    criar_tabela_if_needed >> baixar_e_subir_gcs >> carregar_temp_table >> merge_para_tabela_principal >> drop_temp_table
