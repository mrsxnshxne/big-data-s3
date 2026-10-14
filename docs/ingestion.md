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

## Publication automatique : plugin OpenCode + cron

`./opencode-analytics.sh` exécute une publication : il charge `.env`, choisit
le Python du venv et transmet ses arguments à `produce.py`
(`./opencode-analytics.sh --dry-run` fonctionne).

La publication est déclenchée par le plugin OpenCode
`opencode-plugin/observatory.js` : à la fin de chaque réponse de l'agent, il
lance `opencode-analytics.sh`. Pour l'installer, puis redémarrer OpenCode :

```sh
ln -s "$PWD/opencode-plugin/observatory.js" ~/.config/opencode/plugins/
```

Une entrée cron horaire sert de filet de sécurité (Redpanda ou tunnel
indisponible au moment d'une réponse). Le script charge lui-même `.env`,
puisque cron ne lit pas l'environnement du shell (`crontab -e`) :

```cron
0 * * * * /chemin/vers/big-data-s3/opencode-analytics.sh >> /chemin/vers/big-data-s3/opencode-analytics.log 2>&1
```

Fonctionnement du plugin :

- il s'abonne aux événements OpenCode 2 (`ctx.event.subscribe`) et réagit à
  `session.execution.succeeded`, `failed` et `interrupted`, c'est-à-dire à la
  fin d'une réponse, y compris partielle ;
- les événements sont regroupés pendant 2 secondes (sous-agents qui terminent
  ensemble), puis une seule publication est lancée ; si une publication est
  déjà en cours, une seule autre est relancée à sa suite ;
- il ne fait que **déclencher** `produce.py` : l'API des plugins d'OpenCode 2
  ne permet pas de lister les messages d'une session, et réutiliser le
  producteur évite de dupliquer en JavaScript l'extraction et la
  sérialisation Avro ;
- ses messages (`[plugin] ready…`, erreurs) et la sortie du producteur sont
  ajoutés à `opencode-analytics.log`.

Les doublons possibles (plugin et cron simultanés) sont sans effet : les
lecteurs gardent la dernière version de chaque ligne.

Le sink, lui, tourne en continu : `python3 sink.py` ou
`docker-compose up -d sink`.
