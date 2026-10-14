# Architecture et flux

## Flux principal

```text
~/.local/share/opencode/opencode.db
        |
        v
  produce.py (watermark incrémental, déclenché par le plugin OpenCode
        |      à chaque fin de réponse, et par cron en rattrapage)
        |
        v
  Redpanda : topics opencode.sessions/messages/parts/tools (Avro)
        |     Schema Registry : un schéma par topic (schemas/*.avsc)
        |
        v
  sink.py (consommateur, décodage Avro, batch, append Iceberg)
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
- Le Schema Registry matérialise le contrat entre producteur et sink : un
  événement qui ne respecte pas le schéma Avro est refusé à la production.
- Iceberg est la seule source de vérité lue par le dashboard : le stockage S3
  est atteint *via* le catalogue, ce qui garantit des snapshots cohérents.
- Comme le sink fait de l'append en au-moins-une-fois, chaque lecteur garde la
  version la plus récente de chaque ligne (`updated_at` décroissant).

## Services Docker

`docker-compose.yml` définit six services :

- `redpanda` expose le protocole Kafka sur `localhost:9092` (hôte) et
  `redpanda:29092` (réseau Compose), ainsi que son Schema Registry sur
  `localhost:8081` / `redpanda:8081`.
- `rustfs` expose l'API S3 sur `localhost:9000` et sa console sur
  `localhost:9001`.
- `iceberg-rest` expose le catalogue Iceberg sur `localhost:8181` et utilise
  RustFS comme warehouse. Son catalogue SQLite est persisté dans le volume
  `iceberg-catalog`.
- `sink` consomme les topics et alimente Iceberg en continu.
- `dashboard` est activé par le profil Docker `dashboard` et écoute sur
  `localhost:8501`.
- `spark` est activé par le profil `spark` et exécute `spark/analytics.py`.

Les volumes Docker persistent les objets RustFS (`rustfs-data`), les logs
Redpanda et le Schema Registry (`redpanda-data`) et le catalogue Iceberg
(`iceberg-catalog`). Les ports sont liés à `127.0.0.1` par défaut afin
de ne pas publier les services sur le réseau local.

## Modes de lecture du dashboard

Le dashboard scanne les tables Iceberg du catalogue (`ICEBERG_CATALOG_URI`),
les enregistre dans DuckDB sous `raw_sessions`, `raw_messages`,
`raw_parts` et `raw_tools`, puis crée des vues dédupliquées `sessions`,
`messages`, `parts` et `tools`. Il n'y a plus de repli Parquet local : sans
Iceberg, le dashboard affiche une erreur qui rappelle les étapes du pipeline.

## Synchronisation continue

Le plugin `opencode-plugin/observatory.js` lance `opencode-analytics.sh`
(donc `produce.py`) dès qu'une réponse de l'agent se termine ; une entrée cron
horaire rattrape ce qui aurait été manqué. Le watermark
par dataset est persisté dans `PRODUCER_STATE_FILE` ; l'état d'avancement de
l'écriture Iceberg est, lui, l'offset Kafka du groupe
`KAFKA_CONSUMER_GROUP`.
