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
    'tmdb_details_movies_silver',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['tmdb', 'silver', 'movies']
) as dag:

    create_details_movies_silver_table = BigQueryInsertJobOperator(
        task_id='create_tmdb_details_movies_silver',
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS `engestudo.cinema_silver.tmdb_movies_details`
                PARTITION BY DATE(job_date)
                CLUSTER BY updated_date AS
                SELECT
                  CAST(NULL AS INT64) AS id,
                  CAST(NULL AS INT64) AS id_colecao,
                  CAST(NULL AS STRING) AS imdb_url,
                  CAST(NULL AS STRING) AS titulo_original,
                  CAST(NULL AS STRING) AS titulo,
                  CAST(NULL AS STRING) AS nome_colecao,
                  CAST(NULL AS STRING) AS generos,
                  CAST(NULL AS STRING) AS sinopse,
                  CAST(NULL AS INT64) AS duracao,
                  CAST(NULL AS INT64) AS orcamento,
                  CAST(NULL AS INT64) AS receita,
                  CAST(NULL AS STRING) AS status_lancamento,
                  CAST(NULL AS DATE) AS data_lancamento,
                  CAST(NULL AS STRING) AS idioma_original,
                  CAST(NULL AS INT64) AS votos,
                  CAST(NULL AS FLOAT64) AS media_votos,
                  CAST(NULL AS FLOAT64) AS popularidade,
                  CAST(NULL AS STRING) AS url_backdrop,
                  CAST(NULL AS STRING) AS url_backdrop_colecao,
                  CAST(NULL AS STRING) AS url_poster,
                  CAST(NULL AS STRING) AS url_poster_colecao,
                  CAST(NULL AS STRING) AS homepage,
                  CAST(NULL AS TIMESTAMP) AS job_date,
                  CAST(NULL AS TIMESTAMP) AS updated_date
                LIMIT 0;
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    merge_details_movies_silver = BigQueryInsertJobOperator(
        task_id='merge_tmdb_details_movies_silver',
        configuration={
            "query": {
                "query": """
                MERGE `engestudo.cinema_silver.tmdb_movies_details` T
                USING (
                    WITH ultima_atualizacao AS (
                        SELECT MAX(updated_date) AS ultima_data
                        FROM `engestudo.cinema_silver.tmdb_movies_details`
                    ),
                    novos_dados AS (
                        SELECT * FROM (
                            SELECT
                                tmd.id,
                                SAFE_CAST(tmd.id_collection AS INT64) AS id_colecao,
                                IF(tmd.imdb_id IS NOT NULL AND tmd.imdb_id != '', CONCAT('https://www.imdb.com/pt/title/', tmd.imdb_id), NULL) AS imdb_url,
                                tmd.original_title AS titulo_original,
                                tmd.title AS titulo,
                                tmd.name_collection AS nome_colecao,
                                tmd.genres_name AS generos,
                                tmd.overview AS sinopse,
                                tmd.runtime AS duracao,
                                tmd.budget AS orcamento,
                                tmd.revenue AS receita,
                                tmd.status AS status_lancamento,
                                tmd.release_date AS data_lancamento,
                                tmd.original_language AS idioma_original,
                                tmd.vote_count AS votos,
                                tmd.vote_average AS media_votos,
                                tmd.popularity AS popularidade,
                                IF(tmd.backdrop_path IS NOT NULL, CONCAT('https://image.tmdb.org/t/p/original', tmd.backdrop_path), NULL) AS url_backdrop,
                                IF(tmd.backdrop_path_collection IS NOT NULL, CONCAT('https://image.tmdb.org/t/p/original', tmd.backdrop_path_collection), NULL) AS url_backdrop_colecao,
                                IF(tmd.poster_path IS NOT NULL, CONCAT('https://image.tmdb.org/t/p/original', tmd.poster_path), NULL) AS url_poster,
                                IF(tmd.poster_path_collection IS NOT NULL, CONCAT('https://image.tmdb.org/t/p/original', tmd.poster_path_collection), NULL) AS url_poster_colecao,
                                tmd.homepage,
                                tmd.job_date,
                                tmd.updated_date,
                                ROW_NUMBER() OVER (PARTITION BY tmd.id ORDER BY tmd.updated_date DESC) AS n_linha
                            FROM `engestudo.cinema_bronze.tmdb_movies_details` tmd,
                                 ultima_atualizacao ua
                            WHERE tmd.updated_date >= IFNULL(ua.ultima_data, TIMESTAMP('1970-01-01'))
                        ) WHERE n_linha = 1
                    )
                    SELECT * FROM novos_dados
                ) B
                ON T.id = B.id
                WHEN MATCHED THEN
                  UPDATE SET
                    id_colecao = B.id_colecao,
                    imdb_url = B.imdb_url,
                    titulo_original = B.titulo_original,
                    titulo = B.titulo,
                    nome_colecao = B.nome_colecao,
                    generos = B.generos,
                    sinopse = B.sinopse,
                    duracao = B.duracao,
                    orcamento = B.orcamento,
                    receita = B.receita,
                    status_lancamento = B.status_lancamento,
                    data_lancamento = B.data_lancamento,
                    idioma_original = B.idioma_original,
                    votos = B.votos,
                    media_votos = B.media_votos,
                    popularidade = B.popularidade,
                    url_backdrop = B.url_backdrop,
                    url_backdrop_colecao = B.url_backdrop_colecao,
                    url_poster = B.url_poster,
                    url_poster_colecao = B.url_poster_colecao,
                    homepage = B.homepage,
                    job_date = B.job_date,
                    updated_date = B.updated_date
                WHEN NOT MATCHED THEN
                  INSERT (
                    id, id_colecao, imdb_url, titulo_original, titulo, nome_colecao, generos, sinopse, duracao, orcamento, receita, status_lancamento, data_lancamento, idioma_original, votos, media_votos, popularidade, url_backdrop, url_backdrop_colecao, url_poster, url_poster_colecao, homepage, job_date, updated_date
                  )
                  VALUES (
                    B.id, B.id_colecao, B.imdb_url, B.titulo_original, B.titulo, B.nome_colecao, B.generos, B.sinopse, B.duracao, B.orcamento, B.receita, B.status_lancamento, B.data_lancamento, B.idioma_original, B.votos, B.media_votos, B.popularidade, B.url_backdrop, B.url_backdrop_colecao, B.url_poster, B.url_poster_colecao, B.homepage, B.job_date, B.updated_date
                  );
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    verificar_integridade_silver = BigQueryInsertJobOperator(
        task_id='verificar_integridade_tmdb_movies_silver',
        configuration={
            "query": {
                "query": """
                SELECT id, COUNT(*) AS qtd
                FROM `engestudo.cinema_silver.tmdb_movies_details`
                GROUP BY id
                HAVING COUNT(*) > 1;
                """,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    trigger_tmdb_details_movies_gold = TriggerDagRunOperator(
        task_id='trigger_tmdb_details_movies_gold',
        trigger_dag_id='tmdb_details_movies_gold',
        execution_date='{{ ds }}',
        wait_for_completion=False,
        reset_dag_run=True
    )

    create_details_movies_silver_table >> merge_details_movies_silver >> verificar_integridade_silver >> trigger_tmdb_details_movies_gold
