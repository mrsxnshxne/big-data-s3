# OpenCode Observatory

Pipeline local d'analyse des conversations OpenCode et Claude Code. Le projet
extrait la base SQLite locale d'OpenCode depuis un plugin OpenCode et les
transcripts de Claude Code (`~/.claude/projects`), publie les changements sous forme d'événements dans
un broker Redpanda, les écrit en continu dans des tables Apache Iceberg
(stockées sur un objet S3 compatible) et les interroge avec DuckDB dans un
dashboard Streamlit.

```text
opencode.db                      ~/.claude/projects/*.jsonl
    -> plugin OpenCode               -> producteur Claude Code (hook Stop)
       (fin de chaque réponse, watermark incrémental, Avro, colonne source)
    -> Redpanda (topics opencode.sessions/messages/parts/tools)
    -> sink.py (append par lots)
    -> Apache Iceberg REST
    -> RustFS / S3 (warehouse)
    -> DuckDB + déduplication
    -> Streamlit
```

Apache Spark lit les mêmes tables Iceberg et fournit des agrégats analytiques :
`daily_activity`, `session_summary`, `tool_usage` et `model_usage`.

> Ce projet n'est pas un scraper web. Il collecte les données déjà présentes
> dans la base SQLite locale d'OpenCode.

## Documentation

- [Documentation complète](docs/README.md)
- [Architecture et flux](docs/architecture.md)
- [Temps réel : Arroyo et panel SSE](docs/realtime.md)
- [Extraction et producteur d'événements](docs/ingestion.md)
- [Redpanda et flux d'événements](docs/redpanda.md)
- [RustFS et S3](docs/s3.md)
- [Apache Iceberg](docs/iceberg.md)
- [Data Lake, Data Warehouse et Lakehouse](docs/concepts.md)
- [Spark et agrégats](docs/spark.md)
- [Dashboard et exploitation](docs/operations.md)

Les données peuvent contenir des prompts, chemins, commandes et sorties d'outils
sensibles. Ne rendez pas les buckets publics et ne commitez jamais `.env`.

## Temps réel

`docker-compose up -d arroyo live && docker-compose run --rm arroyo-deploy` démarre Arroyo et le panel SSE
sur <http://localhost:8600> (détails dans [docs/realtime.md](docs/realtime.md)).

## Installation

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Modifiez `.env` si nécessaire. Le fichier `.env` ne doit jamais être commité.

## Démarrage local

```sh
docker-compose up -d                  # rustfs, iceberg-rest, redpanda (+ registry, HTTP proxy), topics, sink
node plugins/opencode/observatory.js  # historique OpenCode
node plugins/claude/observatory.js    # historique Claude Code
streamlit run app.py --server.address=127.0.0.1
```

Dashboard : <http://localhost:8501>

### Plugins de collecte

Les producteurs sont dans [`plugins/`](plugins/README.md), un dossier par
outil. Chacun a son guide d'installation et de désinstallation :

| Outil | Guide |
| --- | --- |
| OpenCode | [plugins/opencode/README.md](plugins/opencode/README.md#installation) |
| Claude Code | [plugins/claude/README.md](plugins/claude/README.md#installation) |

> Si le plugin OpenCode a été installé quand il était dans `opencode-plugin/`,
> son lien est cassé : voir
> [la migration](plugins/opencode/README.md#migration-depuis-opencode-plugin).

Les lignes portent
`source = "claude-code"` ; le dashboard propose un filtre **Source**
(Tous / OpenCode / Claude Code). Le coût Claude Code est **estimé** à partir
des tarifs API par modèle.

Le sink peut aussi tourner hors Docker dans un terminal dédié :

```sh
python3 sink.py
```

## Dashboard Docker

Le dashboard peut aussi tourner dans Docker et lire Iceberg via le catalogue :

```sh
docker-compose --profile dashboard up --build dashboard
```

Le service est exposé sur `127.0.0.1:8501`. Pour l'ouvrir au réseau local (par exemple
`http://homelab:8501`), mettez `DASHBOARD_BIND=0.0.0.0` dans `.env` : il affiche
les prompts et sorties d'outils, à réserver à un réseau de confiance. Sinon,
utilisez un tunnel SSH :

```sh
ssh -N -L 8501:127.0.0.1:8501 utilisateur@serveur
```

La synchronisation alimente les tables `analytics.sessions`,
`analytics.messages`, `analytics.parts` et `analytics.tools` dans le catalogue
Iceberg. Le dashboard Docker les lit via `iceberg-rest`; en local, `.env`
utilise `localhost:8181`.

## Couche Spark

En local, avec Java et PySpark installés :

```sh
pip install -r spark/requirements.txt
set -a; . ./.env; set +a
./spark/submit.sh --output data/spark
```

Pour exécuter Spark dans Docker et lire les tables Iceberg du warehouse :

```sh
docker-compose --profile spark run --rm spark
```

Le job lit le catalogue Iceberg (`ICEBERG_CATALOG_URI`) et déduplique chaque
table par identifiant. Sortie par défaut dans le warehouse :
`SPARK_OUTPUT=s3a://iceberg-warehouse/spark`.
