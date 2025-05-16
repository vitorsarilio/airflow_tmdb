from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vitor Sarilio',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'tmdb_details_movies_gold',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'gold', 'movies']
) as dag:

    criar_dim_filme = BigQueryInsertJobOperator(
        task_id='criar_dim_filme',
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS `engestudo.cinema_gold.dim_filme`
                PARTITION BY DATE(updated_date)
                CLUSTER BY id AS
                SELECT
                  CAST(NULL AS INT64) AS id,
                  CAST(NULL AS INT64) AS id_colecao,
                  CAST(NULL AS STRING) AS titulo,
                  CAST(NULL AS STRING) AS titulo_original,
                  CAST(NULL AS STRING) AS status_lancamento,
                  CAST(NULL AS STRING) AS nome_colecao,
                  CAST(NULL AS DATE) AS data_lancamento,
                  CAST(NULL AS STRING) AS idioma_original,
                  CAST(NULL AS STRING) AS generos,
                  CAST(NULL AS STRING) AS sinopse,
                  CAST(NULL AS STRING) AS homepage,
                  CAST(NULL AS INT64) AS orcamento,
                  CAST(NULL AS INT64) AS receita,
                  CAST(NULL AS INT64) AS duracao,
                  CAST(NULL AS STRING) AS imdb_url,
                  CAST(NULL AS STRING) AS url_poster,
                  CAST(NULL AS STRING) AS url_backdrop,
                  CAST(NULL AS STRING) AS url_backdrop_colecao,
                  CAST(NULL AS STRING) AS url_poster_colecao,
                  CAST(NULL AS TIMESTAMP) AS updated_date
                LIMIT 0;
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    merge_dim_filme = BigQueryInsertJobOperator(
        task_id='merge_dim_filme',
        configuration={
            "query": {
                "query": """
                MERGE `engestudo.cinema_gold.dim_filme` T
                USING (
                    SELECT
                        id,
                        id_colecao,
                        titulo,
                        titulo_original,
                        status_lancamento,
                        nome_colecao,
                        data_lancamento,
                        idioma_original,
                        generos,
                        sinopse,
                        homepage,
                        orcamento,
                        receita,
                        duracao,
                        imdb_url,
                        url_poster,
                        url_backdrop,
                        url_backdrop_colecao,
                        url_poster_colecao,
                        updated_date
                    FROM `engestudo.cinema_silver.tmdb_movies_details`
                ) S
                ON T.id = S.id
                WHEN MATCHED THEN
                  UPDATE SET
                    id_colecao = S.id_colecao,
                    titulo = S.titulo,
                    titulo_original = S.titulo_original,
                    status_lancamento = S.status_lancamento,
                    nome_colecao = S.nome_colecao,
                    data_lancamento = S.data_lancamento,
                    idioma_original = S.idioma_original,
                    generos = S.generos,
                    sinopse = S.sinopse,
                    homepage = S.homepage,
                    orcamento = S.orcamento,
                    receita = S.receita,
                    duracao = S.duracao,
                    imdb_url = S.imdb_url,
                    url_poster = S.url_poster,
                    url_backdrop = S.url_backdrop,
                    url_backdrop_colecao = S.url_backdrop_colecao,
                    url_poster_colecao = S.url_poster_colecao,
                    updated_date = S.updated_date
                WHEN NOT MATCHED THEN
                  INSERT (
                    id, id_colecao, titulo, titulo_original, status_lancamento, nome_colecao, data_lancamento, idioma_original, generos, sinopse, homepage, orcamento, receita, duracao, imdb_url, url_poster, url_backdrop, url_backdrop_colecao, url_poster_colecao, updated_date
                  )
                  VALUES (
                    S.id, S.id_colecao, S.titulo, S.titulo_original, S.status_lancamento, S.nome_colecao, S.data_lancamento, S.idioma_original, S.generos, S.sinopse, S.homepage, S.orcamento, S.receita, S.duracao, S.imdb_url, S.url_poster, S.url_backdrop, S.url_backdrop_colecao, S.url_poster_colecao, S.updated_date
                  );
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    criar_fato_filme = BigQueryInsertJobOperator(
        task_id='criar_fato_filme',
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS `engestudo.cinema_gold.fato_filme`
                PARTITION BY DATE(job_date)
                CLUSTER BY id AS
                SELECT
                  CAST(NULL AS INT64) AS id,
                  CAST(NULL AS INT64) AS votos,
                  CAST(NULL AS FLOAT64) AS media_votos,
                  CAST(NULL AS FLOAT64) AS popularidade,
                  CAST(NULL AS TIMESTAMP) AS job_date
                LIMIT 0;
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    inserir_fato_filme = BigQueryInsertJobOperator(
        task_id='inserir_fato_filme',
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `engestudo.cinema_gold.fato_filme` (id, votos, media_votos, popularidade, job_date)
                SELECT
                    id,
                    votos,
                    media_votos,
                    popularidade,
                    CURRENT_TIMESTAMP() AS job_date
                FROM `engestudo.cinema_silver.tmdb_movies_details`;
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    criar_dim_filme >> merge_dim_filme >> criar_fato_filme >> inserir_fato_filme
