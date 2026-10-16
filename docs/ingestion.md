# Extraction et producteur d'événements

Deux producteurs alimentent les mêmes topics :

- le plugin OpenCode `plugins/opencode/observatory.js`, détaillé ci-dessous ;
- le producteur Claude Code `plugins/claude/observatory.js`, qui lit les
  transcripts `~/.claude/projects/**/*.jsonl` et réutilise l'encodeur Avro et
  le client Redpanda du premier (voir [son README](../plugins/claude/README.md)).

Chaque événement porte une colonne `source` (`opencode` ou `claude-code`).
Les événements publiés avant l'ajout de cette colonne n'en ont pas ; les
lecteurs les rattachent à `opencode`. Le sink ajoute la colonne aux tables
Iceberg existantes lors du premier append qui la contient.

Pour le plugin OpenCode, installation, désinstallation, configuration et
dépannage sont décrits dans [son README](../plugins/opencode/README.md) ; la
suite de cette page détaille les données qu'il produit.

## Source

Le plugin ouvre la base SQLite d'OpenCode en mode lecture seule. Le chemin
par défaut est `~/.local/share/opencode/opencode.db`, surchargeable par
`OPENCODE_DB`.

Les tables source utilisées sont `session_v2` et `session_message` (schéma
OpenCode v2). Les parts d'un message (texte, raisonnement, appels d'outils)
sont stockées dans le tableau JSON `content` de `session_message.data` et sont
éclatées par le producteur dans les datasets `parts` et `tools`. Les colonnes
JSON de la source sont décodées avec des valeurs de repli afin qu'une donnée
partiellement vide ne bloque pas toute l'extraction.

L'API des plugins d'OpenCode 2 diffuse des événements (fin de réponse, etc.)
mais ne permet pas de lister les messages d'une session : les événements
servent donc de **déclencheur**, et SQLite de **source**.

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
champs encodés. Les timestamps SQLite en millisecondes deviennent des
`timestamp-micros` UTC ; le champ `updated_at` de chaque table sert à la
déduplication à la lecture. Les valeurs structurées d'outil sont conservées
en texte JSON dans `input` et `output`.

Les événements passent par le HTTP Proxy de Redpanda, au format filaire
Confluent (`0x00`, identifiant du schéma sur 4 octets, puis Avro) : le sink
Python les décode avec `confluent-kafka` sans savoir qu'ils viennent de HTTP.

## Watermark incrémental

Le producteur mémorise, dans `PRODUCER_STATE_FILE`
(`data/state/producer-state.json` par défaut), le dernier `time_updated` envoyé
par dataset. Une synchronisation ne publie donc que les lignes nouvelles ou
modifiées. Le watermark n'avance qu'après l'acquittement de tous les
événements par Redpanda : en cas d'échec, les mêmes lignes sont renvoyées à la
synchronisation suivante, d'où la déduplication à la lecture côté dashboard et
Spark.

## Déclenchement

- à la fin de chaque réponse de l'agent (réussie, en erreur ou interrompue),
  avec un regroupement de 2 secondes ;
- au démarrage d'OpenCode, pour rattraper ce qui n'a pas pu partir.

Une seule synchronisation tourne à la fois. Si Redpanda est injoignable,
l'erreur est journalisée dans `observatory.log` et OpenCode n'est pas
affecté.

## Commandes

Le même fichier s'exécute en ligne de commande (Node 22.13 ou plus récent) :

```sh
# Publier uniquement les nouvelles lignes
node plugins/opencode/observatory.js

# Compter les événements en attente sans contacter Redpanda
node plugins/opencode/observatory.js --dry-run

# Republier tout l'historique (par exemple après une remise à zéro de la stack)
node plugins/opencode/observatory.js --full
```

Le sink, lui, tourne en continu : `python3 sink.py` ou
`docker-compose up -d sink`.
