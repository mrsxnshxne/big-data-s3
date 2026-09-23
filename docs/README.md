# Documentation OpenCode Observatory

## Objectif

OpenCode Observatory transforme l'historique local des conversations OpenCode
en données analytiques consultables. Le pipeline est conçu pour fonctionner en
local avec Docker Compose, tout en utilisant des interfaces proches de celles
d'une plateforme data : stockage objet S3, tables Iceberg, moteur SQL DuckDB,
traitement distribué Spark et interface Streamlit.

## Vue d'ensemble

```text
SQLite OpenCode
    |
    | extraction en lecture seule
    v
Parquet local
    |\
    | \-- RustFS/S3: stockage partagé des fichiers
    |
    \---- Iceberg REST: tables analytiques versionnées
             |
             v
       DuckDB + Streamlit

Parquet local ou S3 -- Apache Spark --> agrégats analytiques
```

Le pipeline principal est déclenché par `opencode-analytics.sh` :

1. `ingest.py` lit `opencode.db` sans la modifier.
2. Les lignes sont normalisées en quatre datasets Parquet.
3. Les fichiers peuvent être envoyés vers RustFS avec l'API S3.
4. Les mêmes datasets peuvent être publiés dans le catalogue Iceberg REST.
5. Le dashboard charge Iceberg en priorité, puis utilise les fichiers Parquet.

## Composants

| Composant | Rôle |
| --- | --- |
| Python, SQLite, pandas | Extraction et normalisation de la base OpenCode |
| PyArrow / Parquet | Format colonne local et portable |
| RustFS | Stockage objet S3-compatible local |
| Apache Iceberg REST | Catalogue et tables analytiques |
| DuckDB | Requêtes SQL locales et jointures du dashboard |
| Streamlit | Visualisation des sessions et appels d'outils |
| Apache Spark | Agrégations batch sur les fichiers Parquet |
| Docker Compose | Exécution reproductible des services |

## Démarrage rapide

```sh
python3 -m venv .venv
. ./venv.sh activate
pip install -r requirements.txt
pip install -r requirements-spark.txt  # nécessaire uniquement pour Spark local
cp .env.example .env
docker-compose up -d rustfs iceberg-rest
set -a; . ./.env; set +a
./opencode-analytics.sh sync
./opencode-spark.sh
streamlit run app.py --server.address=127.0.0.1
```

Le dashboard est disponible sur <http://localhost:8501>. Pour lancer le
dashboard dans Docker :

```sh
docker-compose --profile dashboard up --build dashboard
```

## Guides

- [Architecture et flux](architecture.md)
- [Extraction, schéma et synchronisation](ingestion.md)
- [RustFS et S3](s3.md)
- [Apache Iceberg](iceberg.md)
- [Data Lake, Data Warehouse et Lakehouse](concepts.md)
- [Spark et agrégats](spark.md)
- [Dashboard, exploitation et dépannage](operations.md)

## Limites et sécurité

- L'extraction dépend du schéma SQLite fourni par la version installée
  d'OpenCode.
- La publication Iceberg remplace le contenu des tables analytiques lors de
  chaque synchronisation (`overwrite`).
- Les identifiants de l'exemple sont destinés au développement local et doivent
  être changés dans un environnement partagé.
- Les prompts, chemins, commandes et sorties d'outils peuvent être sensibles.
  Restreindre l'accès réseau à localhost ou à un tunnel authentifié.
