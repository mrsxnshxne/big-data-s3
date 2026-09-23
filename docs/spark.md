# Apache Spark et agrégats

## Rôle

`spark/analytics.py` lit les datasets `sessions`, `messages` et `tools` depuis
un chemin local ou S3A. Il construit quatre DataFrames agrégés puis les écrit
en Parquet avec le mode `overwrite`.

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

```sh
pip install -r requirements-spark.txt
./opencode-spark.sh --input data/parquet --output data/spark
```

Le script utilise `local[*]` par défaut. Le master peut être changé avec
`--master` ou `SPARK_MASTER`.

## Exécution Docker et S3

```sh
docker-compose --profile spark run --rm spark
```

Le profil configure `SPARK_INPUT` sur
`s3a://opencode-analytics/parquet` et `SPARK_OUTPUT` sur
`s3a://opencode-analytics/spark`. Le package Hadoop AWS est fourni à
`spark-submit` par `opencode-spark.sh` ou le service Docker.

## Points d'attention

Les agrégats dépendent des colonnes générées par l'extraction. Il faut donc
relancer l'extraction avant Spark lorsque la base OpenCode a changé. Le job ne
publie pas les agrégats dans Iceberg : il les écrit uniquement au chemin de
sortie configuré.
