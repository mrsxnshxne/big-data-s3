# Dashboard, exploitation et dépannage

## Dashboard

Le dashboard Streamlit permet de :

- sélectionner une session et afficher ses métriques ;
- compter les messages et appels d'outils ;
- consulter les tokens, le raisonnement et le coût ;
- visualiser les types de parties et les outils utilisés ;
- parcourir le fil des messages et les entrées/sorties d'outils.

Les tables sont scannées depuis Iceberg et mises en cache trente secondes. Les
lignes sont dédupliquées dans DuckDB (dernier `updated_at` par identifiant).

Lancement local :

```sh
streamlit run app.py --server.address=127.0.0.1
```

## Variables importantes

| Variable | Valeur par défaut | Usage |
| --- | --- | --- |
| `OPENCODE_DB` | `~/.local/share/opencode/opencode.db` | Base source |
| `PRODUCER_STATE_FILE` | `data/state/producer-state.json` | Watermarks du plugin |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker Redpanda (hôte) |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry Redpanda (hôte) |
| `KAFKA_HTTP_PROXY_URL` | `http://localhost:8082` | HTTP Proxy Redpanda, utilisé par le plugin |
| `KAFKA_TOPIC_PREFIX` | `opencode` | Préfixe des topics |
| `KAFKA_CONSUMER_GROUP` | `opencode-iceberg-sink` | Groupe offsets du sink |
| `SINK_MAX_RECORDS` | `2000` | Taille de lot Iceberg |
| `SINK_FLUSH_INTERVAL_SECONDS` | `5` | Latence max d'un lot |
| `ICEBERG_CATALOG_URI` | `http://localhost:8181` dans `.env.example` | Catalogue REST |
| `ICEBERG_WAREHOUSE` | `s3://iceberg-warehouse` | Warehouse Iceberg |

## Dépannage

### Le dashboard affiche « Aucune donnée dans Iceberg »

Le pipeline complet doit tourner :

```sh
docker-compose up -d rustfs iceberg-rest redpanda redpanda-topics sink
node plugins/opencode/observatory.js
```

Vérifier ensuite `docker logs big-data-s3-sink` (les append doivent défiler).

### Le plugin ne publie pas

Lire `opencode-analytics.log` : le plugin y écrit son chargement
(`ready for …`), chaque publication et chaque erreur. Les causes courantes
(lien absent, HTTP Proxy injoignable, schéma incompatible) sont détaillées
dans [le README du plugin](../plugins/opencode/README.md#dépannage).

### Le producteur ne publie rien

C'est normal si rien n'a changé depuis le dernier watermark. Contrôler avec
`node plugins/opencode/observatory.js --dry-run`. Forcer une republication
complète avec `node plugins/opencode/observatory.js --full`.

### Le sink ne consomme pas

Vérifier la connectivité Kafka : `localhost:9092` depuis l'hôte,
`redpanda:29092` depuis un conteneur. Lister les topics :

```sh
docker exec big-data-s3-redpanda rpk topic list
```

### Iceberg refuse d'écrire

Vérifier que `rustfs` répond, que `ICEBERG_CATALOG_URI` pointe vers
`iceberg-rest`, et que le bucket du warehouse est accessible. Depuis l'hôte,
l'endpoint est `localhost:9000` ; depuis un conteneur Compose,
`http://rustfs:9000`.

### Spark ne trouve pas les tables

Vérifier `ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE` et la présence du package
`iceberg-spark-runtime` dans les `--packages` (`opencode-spark.sh` et le
service Docker l'ajoutent). En Docker, utiliser les chemins `s3a://` et non
`http://localhost:9000` comme chemin de données Spark.

## Cycle d'arrêt

Le plugin OpenCode peut rester installé : stack arrêtée, il journalise
l'échec et rattrapera au prochain démarrage d'OpenCode. Pour l'arrêter
complètement, le désinstaller ([README du plugin](../plugins/opencode/README.md#désinstallation)).

```sh
docker-compose --profile dashboard down
docker-compose --profile spark down
docker-compose down
```

Les volumes `rustfs-data`, `redpanda-data` et `iceberg-catalog` ne sont pas
supprimés par `docker-compose down`.
