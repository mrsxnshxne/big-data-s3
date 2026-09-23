# OpenCode Observatory

Pipeline local d'analyse des conversations OpenCode. Le projet extrait la base
SQLite locale d'OpenCode, transforme les données en jeux Parquet, les stocke
dans un objet S3 compatible, les publie dans un catalogue Apache Iceberg et les
interroge avec DuckDB dans un dashboard Streamlit.

```text
opencode.db
    -> ingest.py
    -> Parquet (sessions, messages, parts, tools)
    -> RustFS / S3
    -> Apache Iceberg REST
    -> DuckDB
    -> Streamlit
```

Apache Spark fournit en parallèle des agrégats analytiques :
`daily_activity`, `session_summary`, `tool_usage` et `model_usage`.

> Ce projet n'est pas un scraper web. Il collecte les données déjà présentes
> dans la base SQLite locale d'OpenCode.

## Documentation

- [Documentation complète](docs/README.md)
- [Architecture et flux](docs/architecture.md)
- [Extraction et modèle de données](docs/ingestion.md)
- [RustFS et S3](docs/s3.md)
- [Apache Iceberg](docs/iceberg.md)
- [Data Lake, Data Warehouse et Lakehouse](docs/concepts.md)
- [Spark et agrégats](docs/spark.md)
- [Dashboard et exploitation](docs/operations.md)

Les données peuvent contenir des prompts, chemins, commandes et sorties d'outils
sensibles. Ne rendez pas les buckets publics et ne commitez jamais `.env`.

## Installation

```sh
python3 -m venv .venv
. ./venv.sh activate
pip install -r requirements.txt
cp .env.example .env
```

Modifiez `.env` si nécessaire. Le fichier `.env` ne doit jamais être commité.

## Démarrage local

```sh
docker-compose up -d rustfs iceberg-rest
set -a; . ./.env; set +a
./opencode-analytics.sh sync
./opencode-spark.sh
streamlit run app.py --server.address=127.0.0.1
```

Dashboard : <http://localhost:8501>

Pour synchroniser automatiquement toutes les 30 secondes :

```sh
./opencode-analytics.sh enable
./opencode-analytics.sh status
./opencode-analytics.sh disable
```

Le venv peut être désactivé avec :

```sh
. ./venv.sh deactivate
```

## Dashboard Docker

Le dashboard peut aussi récupérer les Parquet depuis RustFS :

```sh
docker-compose --profile dashboard up --build dashboard
```

Le service est exposé sur `127.0.0.1:8501`. Pour un serveur distant, utilisez un tunnel SSH :

```sh
ssh -N -L 8501:127.0.0.1:8501 utilisateur@serveur
```

La synchronisation publie les tables `analytics.sessions`, `analytics.messages`,
`analytics.parts` et `analytics.tools` dans le catalogue Iceberg. Le dashboard
Docker les lit via `iceberg-rest`; en local, `.env` utilise `localhost:8181`.

## Couche Spark

En local, avec Java et PySpark installes :

```sh
pip install -r requirements-spark.txt
./opencode-spark.sh --input data/parquet --output data/spark
```

Pour executer Spark dans Docker et lire les Parquet depuis RustFS :

```sh
docker-compose --profile spark run --rm spark
```

Les sorties locales sont ignorées par Git. Pour une sortie S3, utilisez
`SPARK_INPUT=s3a://opencode-analytics/parquet` et
`SPARK_OUTPUT=s3a://opencode-analytics/spark`.
