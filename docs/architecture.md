# Architecture et flux

## Flux principal

```text
~/.local/share/opencode/opencode.db
        |
        v
  ingest.py --database ...
        |
        +--> data/parquet/sessions.parquet
        +--> data/parquet/messages.parquet
        +--> data/parquet/parts.parquet
        +--> data/parquet/tools.parquet
        |          |
        |          +--> RustFS: s3://opencode-analytics/parquet/
        |          |
        |          +--> Iceberg: analytics.sessions/messages/parts/tools
        |
        +--> analytics.py --> data/spark/* ou s3://.../spark/*
                                      |
                                      v
                              dashboard Streamlit
```

## Services Docker

`docker-compose.yml` définit quatre services :

- `rustfs` expose l'API S3 sur `localhost:9000` et sa console sur
  `localhost:9001`.
- `iceberg-rest` expose le catalogue Iceberg sur `localhost:8181` et utilise
  RustFS comme warehouse.
- `dashboard` est activé par le profil Docker `dashboard` et écoute sur
  `localhost:8501`.
- `spark` est activé par le profil `spark` et exécute `spark/analytics.py`.

Les volumes Docker persistent les objets RustFS dans `rustfs-data`. Les ports
sont liés à `127.0.0.1` par défaut afin de ne pas publier le service sur le
réseau local.

## Modes de lecture du dashboard

Le dashboard tente de charger les tables Iceberg lorsque
`ICEBERG_CATALOG_URI` est défini. Les tables disponibles sont enregistrées dans
DuckDB sous les noms `sessions`, `messages`, `parts` et `tools`.

Si une table Iceberg n'est pas disponible, l'application utilise les fichiers
Parquet présents dans `DATA_DIR`. Cela permet le développement local sans
catalogue, mais ne remplace pas la synchronisation de production.

## Synchronisation continue

`opencode-analytics.sh enable` démarre une boucle en arrière-plan. Elle lance
une synchronisation toutes les `OPENCODE_ANALYTICS_INTERVAL` secondes, écrit
son PID dans `OPENCODE_ANALYTICS_PID_FILE` et ses logs dans
`OPENCODE_ANALYTICS_LOG`. La synchronisation est idempotente du point de vue
des datasets : elle reconstruit les Parquet depuis la source puis republie les
tables analytiques.
