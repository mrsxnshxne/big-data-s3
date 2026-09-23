# Apache Iceberg

## Rôle d'Iceberg

Apache Iceberg est un format de table pour data lakes. Il ajoute une couche de
métadonnées au-dessus des fichiers objet : schéma, snapshots, manifestes et
opérations de lecture/écriture cohérentes. Il permet aux moteurs comme Spark,
DuckDB ou Trino de traiter un ensemble de fichiers comme une table analytique.

Iceberg n'est pas le stockage lui-même. Dans ce projet, RustFS stocke les
objets et `iceberg-rest` fournit le catalogue qui sait où se trouvent les
tables.

## Configuration du projet

- Catalogue REST : `http://localhost:8181`
- Namespace : `analytics`
- Warehouse : `s3://iceberg-warehouse`
- Tables : `analytics.sessions`, `analytics.messages`,
  `analytics.parts`, `analytics.tools`

`ingest.py --iceberg` charge le catalogue, crée le namespace si nécessaire,
puis crée ou remplace chaque table à partir du Parquet Arrow. Le premier
chargement construit le schéma Iceberg depuis le schéma PyArrow.

## Pourquoi conserver Parquet et Iceberg ?

Les fichiers Parquet sont simples à inspecter, transférer et consommer par
Spark. Iceberg fournit en plus un contrat de table et un catalogue centralisé.
Le dashboard tente donc Iceberg en priorité et garde le Parquet comme repli
local.

## Conséquence de la synchronisation

La fonction de publication utilise `table.overwrite(arrow_table)`. Chaque
synchronisation représente donc un nouvel état complet des données extraites,
et non un append incrémental. Ce choix évite les doublons mais peut être coûteux
si la base devient volumineuse.

Pour une évolution vers l'incrémental, il faudra définir une clé de déduplication,
une stratégie de suppression et une politique de rétention des snapshots.
