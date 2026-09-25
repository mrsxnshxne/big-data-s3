# Extraction et producteur d'événements

## Source

`produce.py` ouvre la base SQLite d'OpenCode en mode lecture seule. Le chemin
par défaut est `~/.local/share/opencode/opencode.db`, surchargeable par
`OPENCODE_DB` ou `--database`.

Les tables source utilisées sont `session`, `message` et `part`. Les colonnes
JSON de la source sont décodées avec des valeurs de repli afin qu'une donnée
partiellement vide ne bloque pas toute l'extraction.

## Événements produits

| Dataset / topic | Contenu principal |
| --- | --- |
| `sessions` | Identité, titre, répertoire, agent, modèle, fournisseur, coût, tokens, `created_at`, `updated_at` |
| `messages` | Rôle, modèle, statut de fin, coût, tokens, `created_at`, `updated_at` |
| `parts` | Parties textuelles, raisonnement ou outil d'un message |
| `tools` | Nom de l'outil, appel, statut, entrée, sortie et horodatages |

Chaque ligne est publiée sous forme d'événement JSON JSON-lines, clé de
compaction = identifiant de la ligne. Les timestamps SQLite en millisecondes
deviennent des horodatages ISO 8601 UTC ; le champ `updated_at` de chaque table
sert à la déduplication à la lecture. Les valeurs structurées d'outil sont
conservées en texte JSON dans `input` et `output`.

## Watermark incrémental

Le producteur mémorise, dans `PRODUCER_STATE_FILE`
(`data/state/producer-state.json` par défaut), le dernier `time_updated` envoyé
par dataset. Une synchronisation ne publie donc que les lignes nouvelles ou
modifiées. La sauvegarde du watermark a lieu après l'acquittement de tous les
événements par Redpanda (`acks=all`) : en cas de crash, quelques lignes peuvent
être republiées, d'où la déduplication à la lecture côté sink et dashboard.

## Commandes

```sh
# Publier uniquement les nouvelles lignes (comportement par défaut)
python3 produce.py

# Compter les événements en attente sans toucher Redpanda
python3 produce.py --dry-run

# Republier tout l'historique (par exemple après --full côté sink)
python3 produce.py --full
```

## Scripts de synchronisation

- `./opencode-analytics.sh sync` exécute une publication.
- `./opencode-analytics.sh enable` lance la boucle périodique du producteur.
- `./opencode-analytics.sh status` affiche son état.
- `./opencode-analytics.sh disable` l'arrête.
- `./opencode-s3-logs.sh upload` est un alias historique de `sync`.

Le sink, lui, tourne en continu : `python3 sink.py` ou
`docker-compose up -d sink`.
