# Architecture et flux

## Flux principal

```text
~/.local/share/opencode/opencode.db
        |
        v
  produce.py (producteur, watermark incrémental)
        |
        v
  Redpanda : topics opencode.sessions/messages/parts/tools
        |
        v
  sink.py (consommateur, batch, append Iceberg)
        |
        v
  Iceberg REST + warehouse sur RustFS/S3
        |
        v
  DuckDB (déduplication par id) + Streamlit
```

Spark lit les mêmes tables Iceberg (dédupliquées) et produit les agrégats
`daily_activity`, `session_summary`, `tool_usage` et `model_usage`.

## Pourquoi cette forme

- Redpanda découple l'extraction locale de l'écriture des tables : le
  producteur ne fait que publier des événements, le sink avance à son rythme.
- Iceberg est la seule source de vérité lue par le dashboard : le stockage S3
  est atteint *via* le catalogue, ce qui garantit des snapshots cohérents.
- Comme le sink fait de l'append en au-moins-une-fois, chaque lecteur garde la
  version la plus récente de chaque ligne (`updated_at` décroissant).

## Services Docker

`docker-compose.yml` définit six services :

- `redpanda` expose le protocole Kafka sur `localhost:9092` (hôte) et
  `redpanda:29092` (réseau Compose).
- `rustfs` expose l'API S3 sur `localhost:9000` et sa console sur
  `localhost:9001`.
- `iceberg-rest` expose le catalogue Iceberg sur `localhost:8181` et utilise
  RustFS comme warehouse.
- `sink` consomme les topics et alimente Iceberg en continu.
- `dashboard` est activé par le profil Docker `dashboard` et écoute sur
  `localhost:8501`.
- `spark` est activé par le profil `spark` et exécute `spark/analytics.py`.

Les volumes Docker persistent les objets RustFS (`rustfs-data`) et les logs
Redpanda (`redpanda-data`). Les ports sont liés à `127.0.0.1` par défaut afin
de ne pas publier les services sur le réseau local.

## Modes de lecture du dashboard

Le dashboard scanne les tables Iceberg du catalogue (`ICEBERG_CATALOG_URI`),
les enregistre dans DuckDB sous `raw_sessions`, `raw_messages`,
`raw_parts` et `raw_tools`, puis crée des vues dédupliquées `sessions`,
`messages`, `parts` et `tools`. Il n'y a plus de repli Parquet local : sans
Iceberg, le dashboard affiche une erreur qui rappelle les étapes du pipeline.

## Synchronisation continue

`opencode-analytics.sh enable` démarre une boucle en arrière-plan qui lance
`produce.py` toutes les `OPENCODE_ANALYTICS_INTERVAL` secondes. Le watermark
par dataset est persisté dans `PRODUCER_STATE_FILE` ; l'état d'avancement de
l'écriture Iceberg est, lui, l'offset Kafka du groupe
`KAFKA_CONSUMER_GROUP`.
