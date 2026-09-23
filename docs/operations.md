# Dashboard, exploitation et dépannage

## Dashboard

Le dashboard Streamlit permet de :

- sélectionner une session et afficher ses métriques ;
- compter les messages et appels d'outils ;
- consulter les tokens, le raisonnement et le coût ;
- visualiser les types de parties et les outils utilisés ;
- parcourir le fil des messages et les entrées/sorties d'outils.

Les données sont mises en cache trente secondes. La connexion DuckDB est
recréée lorsque la signature des fichiers Parquet change.

Lancement local :

```sh
streamlit run app.py --server.address=127.0.0.1
```

## Variables importantes

| Variable | Valeur par défaut | Usage |
| --- | --- | --- |
| `OPENCODE_DB` | `~/.local/share/opencode/opencode.db` | Base source |
| `DATA_DIR` | `data/parquet` | Parquet local du dashboard |
| `S3_ENDPOINT` | `http://localhost:9000` | Endpoint RustFS/S3 |
| `S3_BUCKET` | `opencode-analytics` | Bucket des Parquet |
| `S3_PREFIX` | `parquet` | Préfixe des fichiers |
| `ICEBERG_CATALOG_URI` | `http://localhost:8181` dans `.env.example` | Catalogue REST |
| `ICEBERG_WAREHOUSE` | `s3://iceberg-warehouse` | Warehouse Iceberg |
| `OPENCODE_ANALYTICS_INTERVAL` | `30` | Intervalle de synchronisation |

## Dépannage

### Le bucket ou l'endpoint est inaccessible

Vérifier `docker-compose ps`, l'endpoint `S3_ENDPOINT`, les clés S3 et que le
service `rustfs` est démarré. Depuis l'hôte, l'endpoint est `localhost:9000` ;
depuis un conteneur Compose, il est `http://rustfs:9000`.

### Le dashboard affiche « Aucune donnée analytique »

Lancer :

```sh
./opencode-analytics.sh sync
```

Puis vérifier la présence de `data/parquet/*.parquet` ou des tables Iceberg.

### La base OpenCode est introuvable

Contrôler `OPENCODE_DB` et passer explicitement `--database` à `ingest.py`. Le
script refuse de démarrer si le fichier source n'existe pas.

### Spark ne trouve pas les fichiers

Vérifier `SPARK_INPUT`, le préfixe S3 et la présence du connecteur
`hadoop-aws`. En Docker, utiliser les chemins `s3a://` et non
`http://localhost:9000` comme chemin de données Spark.

## Cycle d'arrêt

```sh
./opencode-analytics.sh disable
docker-compose --profile dashboard down
docker-compose --profile spark down
docker-compose down
```

Le volume `rustfs-data` n'est pas supprimé par `docker-compose down`.
