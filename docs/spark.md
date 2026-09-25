# Apache Spark et agrégats

## Rôle

`spark/analytics.py` lit les datasets `sessions`, `messages` et `tools` depuis
le catalogue Iceberg alimenté par le sink streaming. Il déduplique chaque table
par identifiant (dernier `updated_at`), construit quatre DataFrames agrégés
puis les écrit en Parquet avec le mode `overwrite`.

Si aucune variable `ICEBERG_CATALOG_URI` n'est définie, le job retombe sur une
lecture Parquet locale via `SPARK_INPUT` (mode développement).

## Jeux analytiques

| Dataset | Mesures |
| --- | --- |
| `daily_activity` | Messages, sessions, tokens et coût par date |
| `session_summary` | Durée, messages, appels d'outils, coût et tokens par session |
| `tool_usage` | Appels, sessions, succès et erreurs par outil |
| `model_usage` | Messages, sessions, tokens et coût par modèle |

`daily_activity` est partitionné par `activity_date`, ce qui réduit les
lectures lorsqu'une période est filtrée.

## Exécution locale

Avec Java et PySpark installés :

```sh
pip install -r requirements-spark.txt
set -a; . ./.env; set +a
./opencode-spark.sh --output data/spark
```

`opencode-spark.sh` ajoute automatiquement le runtime
`iceberg-spark-runtime-3.5_2.12` aux `--packages` lorsque
`ICEBERG_CATALOG_URI` est défini. Le script utilise `local[*]` par défaut ;
le master peut être changé avec `--master` ou `SPARK_MASTER`.

## Exécution Docker et S3

```sh
docker-compose --profile spark run --rm spark
```

Le profil configure le catalogue REST Iceberg, `SPARK_OUTPUT` sur
`s3a://iceberg-warehouse/spark` et fournit les packages `hadoop-aws` et
`iceberg-spark-runtime` à `spark-submit`.

## Points d'attention

Les agrégats dépendent des colonnes publiées par le producteur et écrites par
le sink. Il faut donc laisser le pipeline (producteur + sink) rattraper les
derniers événements avant de lancer Spark. Le job ne publie pas les agrégats
dans Iceberg : il les écrit uniquement au chemin de sortie configuré.
