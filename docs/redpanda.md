# Redpanda et flux d'événements

## Rôle de Redpanda

Redpanda est un broker de messages compatible Kafka, sans JVM. Il sert de
zone de tampon entre le producteur local (`produce.py`) et le sink streaming
(`sink.py`). Le uploader ne publie plus de fichiers vers S3 : il émet des
événements JSON, et c'est le sink qui écrit dans Iceberg, donc dans S3.

```text
opencode.db --> produce.py --> topics Redpanda --> sink.py --> Iceberg --> RustFS/S3
```

## Topics

Quatre topics, préfixés par `KAFKA_TOPIC_PREFIX` (`opencode` par défaut) :

| Topic | Clé message | Contenu |
| --- | --- | --- |
| `opencode.sessions` | `session_id` | Sessions créées ou mises à jour |
| `opencode.messages` | `message_id` | Messages créés ou mis à jour |
| `opencode.parts` | `part_id` | Parties de message |
| `opencode.tools` | `part_id` | Appels d'outils (dérivés des parties `tool`) |

Les topics sont auto-créés par Redpanda (`auto_create_topics_enabled`).

## Sémantique incrémentale

`produce.py` conserve un watermark par dataset dans
`PRODUCER_STATE_FILE` (dernier `time_updated` milliseconde envoyé). Chaque
cycle ne publie que les lignes `time_updated > watermark`. Avec `--full`, tout
l'historique est republié ; avec `--dry-run`, les événements en attente sont
comptés sans contact avec le broker.

## Garanties

- **At-least-once** : le producteur attend l'acquittement de chaque événement
  (`acks=all`) avant de mettre à jour le watermark ; le sink commite ses
  offsets Kafka seulement après un `append` Iceberg réussi.
- Un crash entre les deux peut donc produire des doublons dans Iceberg. La
  déduplication se fait à la lecture : dashboard et Spark gardent la ligne la
  plus récente par identifiant (`row_number() over (partition by id order by
  updated_at desc)`).

## Batch du sink

`sink.py` accumule les événements par topic et publie un lot Iceberg quand
`SINK_MAX_RECORDS` est atteint ou après `SINK_FLUSH_INTERVAL_SECONDS`.
`--once` vide les topics puis s'arrête, utile pour un test ponctuel.

## Démarrage

```sh
docker-compose up -d redpanda
python3 sink.py                 # terminal dédié, ou :
docker-compose up -d sink       # sink dans Docker
./opencode-analytics.sh enable  # boucle productrice toutes les 30 s
```

Le broker hôte est `localhost:9092` ; depuis le réseau Compose, les conteneurs
utilisent `redpanda:29092`. Pour inspecter les topics :

```sh
docker exec big-data-s3-redpanda rpk topic list
docker exec big-data-s3-redpanda rpk topic consume opencode.sessions --num 3
```
