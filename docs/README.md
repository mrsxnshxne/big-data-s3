# Documentation OpenCode Observatory

## Objectif

OpenCode Observatory transforme l'historique local des conversations OpenCode
en données analytiques consultables. Le pipeline est conçu pour fonctionner en
local avec Docker Compose, tout en utilisant des interfaces proches de celles
d'une plateforme data : broker de streaming, stockage objet S3, tables
Iceberg, moteur SQL DuckDB, traitement distribué Spark et interface Streamlit.

## Vue d'ensemble

```text
SQLite OpenCode
    |
    | extraction en lecture seule, watermark incrémental
    v
Redpanda (topics opencode.*)
    |
    | sink streaming, append par lots
    v
Iceberg REST --> warehouse sur RustFS/S3
    |
    +--> DuckDB + Streamlit (déduplication par id)
    +--> Apache Spark --> agrégats analytiques
```

Le flux est continu :

1. `produce.py` lit `opencode.db` sans la modifier et ne publie que les lignes
   nouvelles ou modifiées vers Redpanda.
2. `sink.py` consomme les topics et **append** les événements dans les tables
   Iceberg ; les données physiques atterrissent dans RustFS/S3.
3. Le dashboard lit le catalogue Iceberg et déduplique chaque table par
   identifiant.
4. Spark agrège les mêmes tables dédupliquées.

## Composants

| Composant | Rôle |
| --- | --- |
| Python, SQLite, pandas | Extraction et normalisation de la base OpenCode |
| Redpanda | Broker de streaming compatible Kafka |
| kafka-python-ng | Producteur et consommateur côté Python |
| RustFS | Stockage objet S3-compatible local (warehouse Iceberg) |
| PyIceberg | Écriture des événements dans les tables Iceberg |
| Apache Iceberg REST | Catalogue et tables analytiques |
| DuckDB | Requêtes SQL locales, déduplication et jointures du dashboard |
| Streamlit | Visualisation des sessions et appels d'outils |
| Apache Spark | Agrégations batch sur les tables Iceberg |
| Docker Compose | Exécution reproductible des services |

## Démarrage rapide

```sh
python3 -m venv .venv
. ./venv.sh activate
pip install -r requirements.txt
cp .env.example .env
docker-compose up -d            # rustfs, iceberg-rest, redpanda, sink
set -a; . ./.env; set +a
./opencode-analytics.sh sync
streamlit run app.py --server.address=127.0.0.1
```

Le dashboard est disponible sur <http://localhost:8501>. Pour lancer le
dashboard dans Docker :

```sh
docker-compose --profile dashboard up --build dashboard
```

## Guides

- [Architecture et flux](architecture.md)
- [Extraction et producteur d'événements](ingestion.md)
- [Redpanda et flux d'événements](redpanda.md)
- [RustFS et S3](s3.md)
- [Apache Iceberg](iceberg.md)
- [Data Lake, Data Warehouse et Lakehouse](concepts.md)
- [Spark et agrégats](spark.md)
- [Dashboard, exploitation et dépannage](operations.md)

## Limites et sécurité

- L'extraction dépend du schéma SQLite fourni par la version installée
  d'OpenCode.
- Le pipeline est au-moins-une-fois : les tables Iceberg contiennent des
  versions multiples, dédupliquées à la lecture, jamais supprimées.
- `iceberg-rest` garde son catalogue en mémoire : s'il est recréé, les offsets
  Kafka déjà committés ne sont pas rejoués. Republier l'historique avec
  `python3 produce.py --full` après avoir redémarré le catalogue.
- Il n'y a pas de traitement des suppressions OpenCode ; `produce.py --full`
  republie l'historique complet.
- Les identifiants de l'exemple sont destinés au développement local et doivent
  être changés dans un environnement partagé.
- Les prompts, chemins, commandes et sorties d'outils peuvent être sensibles.
  Restreindre l'accès réseau à localhost ou à un tunnel authentifié.
