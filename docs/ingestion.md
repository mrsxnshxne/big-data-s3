# Extraction et producteur d'événements

## Source

`produce.py` ouvre la base SQLite d'OpenCode en mode lecture seule. Le chemin
par défaut est `~/.local/share/opencode/opencode.db`, surchargeable par
`OPENCODE_DB` ou `--database`.

Les tables source utilisées sont `session_v2` et `session_message` (schéma
OpenCode v2). Les parts d'un message (texte, raisonnement, appels d'outils)
sont stockées dans le tableau JSON `content` de `session_message.data` et sont
éclatées par le producteur dans les datasets `parts` et `tools`. Les colonnes
JSON de la source sont décodées avec des valeurs de repli afin qu'une donnée
partiellement vide ne bloque pas toute l'extraction.

## Événements produits

| Dataset / topic | Contenu principal |
| --- | --- |
| `sessions` | Identité, titre, répertoire, agent, modèle, fournisseur, coût, tokens, `created_at`, `updated_at` |
| `messages` | Rôle, modèle, statut de fin, coût, tokens, `created_at`, `updated_at` |
| `parts` | Parties textuelles, raisonnement ou outil d'un message |
| `tools` | Nom de l'outil, appel, statut, entrée, sortie et horodatages |

Chaque ligne est publiée sous forme d'événement **Avro**, clé de message =
identifiant de la ligne. Le schéma de chaque topic est défini dans
`schemas/<dataset>.avsc` ; c'est aussi lui qui fixe la liste et l'ordre des
colonnes du producteur. Les timestamps SQLite en millisecondes deviennent des
`timestamp-micros` UTC ; le champ `updated_at` de chaque table
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

## Synchronisation périodique (cron)

`./opencode-analytics.sh` exécute une publication : il charge `.env`, choisit
le Python du venv et transmet ses arguments à `produce.py`
(`./opencode-analytics.sh --dry-run` fonctionne). Pour publier automatiquement, ajoutez le script à la crontab (`crontab -e`).
Il charge lui-même `.env`, puisque cron ne lit pas l'environnement du shell :

```cron
* * * * * /chemin/vers/big-data-s3/opencode-analytics.sh >> /chemin/vers/big-data-s3/opencode-analytics.log 2>&1
```

Le sink, lui, tourne en continu : `python3 sink.py` ou
`docker-compose up -d sink`.
