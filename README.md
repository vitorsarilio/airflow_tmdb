# 🎬 Vitor Sarilio Movies DB

**Powered by TMDB**

Este projeto é uma análise exploratória de filmes lançados até hoje, utilizando **Airflow, Google Cloud, BigQuery, Power BI** e outras tecnologias modernas. A arquitetura é orquestrada em containers via **Docker**, permitindo escalabilidade local ou na **Google Cloud Platform**.  

O objetivo principal é fornecer visualizações interativas dos dados de filmes extraídos da **API TMDB**, com transformações automatizadas via **DAGs no Apache Airflow** e consumo final no **Power BI**.

---

## 🚀 Tecnologias Utilizadas

- **Python 3**
- **Apache Airflow** (orquestração)
- **Docker Desktop**
- **Google Cloud Platform (GCP)**
  - Cloud Storage
  - BigQuery
- **Power BI** (consumo e visualização)
- **API TMDB**: [https://www.themoviedb.org/?language=pt-br](https://www.themoviedb.org/?language=pt-br)
- **HTML5/CSS** (visual customizado no Power BI)

---

## 📦 Estrutura do Projeto

```bash
📁 dags/
 ├── tmdb_details_movies_bronze.py
 ├── tmdb_details_movies_silver.py
 ├── tmdb_details_movies_gold.py
 ├── tmdb_popular_movies_*.py
 ├── tmdb_favorites_movies_*.py
 ├── tmdb_watchlist_movies_*.py
 └── ...
```

- DAGs separadas por tipo de dado e camada (bronze, silver, gold)
- Extração em massa usando **ThreadPoolExecutor**
- Dados salvos e versionados no **BigQuery**
- Tabela final `dim_filme` com colunas como `titulo`, `generos`, `orcamento`, `lucro`, `duração`, etc.

---

## 🐳 Como Executar Localmente

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/seu-repositorio.git
   cd seu-repositorio
   ```

2. Suba os containers com Docker:
   ```bash
   docker-compose up -d
   ```

3. Acesse o Apache Airflow:
   ```
   http://localhost:8080
   ```

4. Configure variáveis no Airflow:
   - `tmdb_api_key`
   - `project_id`
   - `dataset_id`

---

## 📊 Visualização (Power BI)

O relatório final no Power BI permite:

- Filtros por datas, coleções e filmes
- Cálculo automático de lucro
- Destaque visual para os filmes mais rentáveis
- Visual escuro customizado em HTML5 (cards personalizados)

![Dashboard Power BI](./imagens/cards.png)

---

## 📸 Screenshots

| Docker Desktop | VS Code | Power BI |
|----------------|---------|----------|
| ![docker](./imagens/docker.png) | ![vscode](./imagens/vscode.png) | ![powerbi](./imagens/cards.png) |

| Airflow UI | BigQuery |
|------------|----------|
| ![airflow](./imagens/airflow.png) | ![bq](./imagens/bigquery.png) |

---

## 🧠 Bibliotecas Python Usadas

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import requests, pandas as pd, os
from datetime import datetime, timedelta
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor
```

---

## 📁 Dataset Final

| Campo              | Tipo     | Descrição                     |
|--------------------|----------|-------------------------------|
| `id`               | INTEGER  | ID do filme                   |
| `titulo`           | STRING   | Título em português           |
| `generos`          | STRING   | Lista de gêneros              |
| `orcamento`        | INTEGER  | Orçamento em dólares          |
| `receita`          | INTEGER  | Receita em dólares            |
| `duracao`          | INTEGER  | Duração em minutos            |
| `lucro`            | COMPUTADO| Receita - Orçamento           |
| ...                | ...      | ...                           |

---

## ☁️ Observações

- A execução pode ser feita 100% local com Docker ou em ambiente cloud com Composer + GCS + BigQuery.
- O Airflow é responsável por toda a orquestração das camadas bronze → silver → gold.
- As tabelas no BigQuery estão particionadas por `updated_date`.

---

## 📬 Contato

Projeto desenvolvido por **Vitor Sarilio**  
🔗 [LinkedIn](https://www.linkedin.com/in/vitorsarilio)  
📧 Email: vitor.sarilio@email.com (exemplo)

---

> **Powered by** [TMDB API](https://www.themoviedb.org/)