# Extraction et modèle de données

## Source

`ingest.py` ouvre la base SQLite d'OpenCode en mode lecture seule. Le chemin par
défaut est `~/.local/share/opencode/opencode.db`, surchargeable par
`OPENCODE_DB` ou `--database`.

Les tables source utilisées sont `session`, `message` et `part`. Les colonnes
JSON de la source sont décodées avec des valeurs de repli afin qu'une donnée
partiellement vide ne bloque pas toute l'extraction.

## Datasets produits

| Dataset | Contenu principal |
| --- | --- |
| `sessions` | Identité, titre, répertoire, agent, modèle, fournisseur, coût et compteurs de tokens |
| `messages` | Rôle, modèle, statut de fin, coût et tokens par message |
| `parts` | Parties textuelles, raisonnement ou outil d'un message |
| `tools` | Nom de l'outil, appel, statut, entrée, sortie et horodatages |

Les timestamps SQLite en millisecondes sont convertis en timestamps UTC. Les
valeurs structurées d'outil sont conservées sous forme JSON texte dans
`input` et `output`, ce qui permet de préserver le détail sans imposer un
schéma rigide.

## Commandes

Extraction locale uniquement :

```sh
python3 ingest.py --database "$OPENCODE_DB" --output data/parquet
```

Extraction et upload S3 :

```sh
python3 ingest.py --database "$OPENCODE_DB" \
  --output data/parquet --upload
```

Extraction, upload S3 et publication Iceberg :

```sh
python3 ingest.py --database "$OPENCODE_DB" \
  --output data/parquet --upload --iceberg
```

Le script crée le bucket S3 configuré s'il n'existe pas. La publication crée
le namespace `analytics` et les tables manquantes. Les tables déjà existantes
sont remplacées par le contenu extrait courant.

## Scripts de synchronisation

- `./opencode-analytics.sh sync` exécute une synchronisation.
- `./opencode-analytics.sh enable` lance la synchronisation périodique.
- `./opencode-analytics.sh status` affiche son état.
- `./opencode-analytics.sh disable` l'arrête.
- `./opencode-s3-logs.sh upload` est un alias historique de `sync`.
