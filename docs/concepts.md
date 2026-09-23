# Data Lake, Data Warehouse et Lakehouse

## Data Lake

Un **Data Lake** est un espace de stockage centralisé qui conserve des données
brutes ou peu transformées, souvent dans un stockage objet comme S3. Il accepte
plusieurs formats et laisse le schéma être appliqué au moment de la lecture
(*schema-on-read*).

Dans ce projet, RustFS joue le rôle de stockage de data lake pour les Parquet
extraits d'OpenCode et pour le warehouse Iceberg.

Avantages : coût et souplesse, conservation des détails, séparation du stockage
et du calcul. Risques : fichiers difficiles à gouverner, schémas instables et
duplication si les conventions ne sont pas documentées.

## Data Warehouse

Un **Data Warehouse** est une base optimisée pour l'analyse, généralement avec
des tables structurées, des règles de qualité et un schéma défini avant le
chargement (*schema-on-write*). Il vise des requêtes fiables et des indicateurs
partagés.

Le projet n'utilise pas un entrepôt serveur classique. DuckDB fournit un moteur
SQL embarqué et les tables Iceberg apportent une structure analytique au-dessus
du stockage objet.

## Lakehouse

Un **Lakehouse** combine la souplesse d'un data lake avec des garanties
habituellement associées à un warehouse : schéma de table, transactions,
évolution de schéma, snapshots et catalogue. Iceberg est l'une des technologies
qui permettent ce modèle.

L'architecture OpenCode Observatory est donc un petit lakehouse local :

```text
RustFS/S3 = stockage objet du lake
Iceberg   = couche de tables et métadonnées
DuckDB    = moteur SQL de consommation
Spark     = moteur de transformation batch
```

## ETL et ELT

L'extraction SQLite vers Parquet est une étape **ETL** légère : les champs JSON
sont décodés et normalisés avant d'être déposés. Les agrégations Spark sont
plus proches de l'**ELT** : les données sont déposées puis transformées dans
le moteur analytique.
