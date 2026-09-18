# OpenCode Observatory

Pipeline local d'analyse des conversations OpenCode :

`opencode.db` -> Parquet -> RustFS/S3 -> Iceberg REST -> DuckDB -> Streamlit

Spark produit une couche d'agregats depuis les Parquet :
`daily_activity`, `session_summary`, `tool_usage` et `model_usage`.

Les données peuvent contenir des prompts, chemins, commandes et sorties d'outils sensibles. Ne rendez pas le bucket public.

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

Les sorties locales sont ignorees par Git. Pour une sortie S3, utilisez
`SPARK_INPUT=s3a://opencode-analytics/parquet` et
`SPARK_OUTPUT=s3a://opencode-analytics/spark`.
