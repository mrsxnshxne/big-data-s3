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

- Catalogue : REST `http://localhost:8181` (`ICEBERG_CATALOG_TYPE=sql` accepté
  pour les tests hors Docker)
- Namespace : `analytics`
- Warehouse : `s3://iceberg-warehouse`
- Tables : `analytics.sessions`, `analytics.messages`,
  `analytics.parts`, `analytics.tools`

Ce n'est plus `produce.py` qui écrit dans Iceberg : le sink streaming charge le
catalogue, crée le namespace et les tables manquantes (schéma dérivé d'un
schéma PyArrow explicite dans `sink.py`), puis **append** les lots
d'événements consommés depuis Redpanda.

## Pourquoi Iceberg plutôt que S3 brut pour le dashboard ?

Le dashboard pourrait théoriquement lister des fichiers Parquet sur S3, mais il
perdrait alors la notion de table : snapshots cohérents, schéma versionné,
évolutions de colonnes et garantie que l'on lit un état complet. En passant par
le catalogue Iceberg, Streamlit lit exactement le dernier snapshot validé par
le sink, et S3 reste un détail de stockage.

## Sémantique append et déduplication

Chaque événement Redpanda est écrit tel quel (`table.append`) ; une ligne
modifiée produit donc une version supplémentaire dans la table. Les lecteurs
dédupliquent à la lecture :

```sql
select * exclude (_version) from (
  select *, row_number() over (
    partition by session_id order by updated_at desc
  ) as _version
  from raw_sessions
)
where _version = 1
```

Le même motif est appliqué dans `app.py` (DuckDB) et `spark/analytics.py`
(Spark). Cette approche est idempotente et tolère la sémantique
au-moins-une-fois du pipeline.

## Rétention

Les snapshots s'accumulent avec les append. Une politique de purge
(`expire_snapshots`) sera à ajouter quand le volume de données le justifiera ;
le projet ne la met pas en œuvre pour l'instant.
