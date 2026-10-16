# Plugin OpenCode Observatory

`observatory.js` est le **producteur** du pipeline. Il tourne dans OpenCode
et publie dans Redpanda chaque conversation dès qu'une réponse de l'agent se
termine. Le même fichier sert aussi d'outil en ligne de commande pour publier
à la main, compter les événements en attente ou tout republier.

```text
réponse terminée ─▶ observatory.js ─▶ Schema Registry (schéma Avro)
                         │
                         └──────────▶ HTTP Proxy Redpanda ─▶ topics opencode.*
                                                              │
                                     sink.py ◀────────────────┘ ─▶ Iceberg / S3
```

## Ce qu'il fait

À chaque synchronisation :

1. **Lit la base SQLite d'OpenCode en lecture seule**
   (`~/.local/share/opencode/opencode.db`) : tables `session_v2` et
   `session_message`, uniquement les lignes modifiées depuis le dernier envoi.
2. **Transforme** ces lignes en quatre datasets : `sessions`, `messages`,
   `parts` (texte, raisonnement, outils, éclatés depuis le JSON de chaque
   message) et `tools` (appels d'outils avec entrée, sortie et statut).
3. **Encode chaque ligne en Avro** selon `schemas/<dataset>.avsc`. Une ligne qui
   ne respecte pas le schéma fait échouer la synchronisation avant tout envoi.
4. **Enregistre le schéma** dans le Schema Registry (sujet
   `opencode.<dataset>-value`), qui renvoie son identifiant.
5. **Envoie les événements** au HTTP Proxy de Redpanda, par lots d'environ
   512 Ko, au format filaire Confluent (`0x00` + id du schéma + Avro) : le sink
   Python les décode comme s'ils venaient d'un client Kafka classique.
6. **Avance le watermark** (`data/state/producer-state.json`) seulement une fois
   tous les événements acquittés. En cas d'échec, rien n'est perdu : les
   mêmes lignes sont renvoyées à la synchronisation suivante.

### Quand il se déclenche

| Moment | Pourquoi |
| --- | --- |
| Fin d'une réponse (`session.execution.succeeded`, `failed`, `interrupted`) | Publier au fil de l'eau, y compris une réponse interrompue |
| Démarrage d'OpenCode | Rattraper ce qui a été écrit pendant que Redpanda était injoignable |

Les déclenchements sont regroupés pendant 2 secondes (des sous-agents qui
terminent ensemble ne lancent qu'un envoi), et deux synchronisations ne
tournent jamais en parallèle : si une réponse se termine pendant un envoi, un
seul nouvel envoi est lancé à sa suite.

Si Redpanda est injoignable, l'erreur est écrite dans le journal et OpenCode
n'est pas affecté. Les données partent à la réponse suivante ou au prochain
démarrage.

### Pourquoi ces choix

- **Aucune dépendance npm.** SQLite est fourni par le runtime (`bun:sqlite`
  dans OpenCode, `node:sqlite` en ligne de commande), l'encodage Avro des types
  utilisés (`string`, `long`, `double`, `null`) tient en quelques fonctions, et
  le réseau passe par `fetch`. Rien à installer, rien de natif à compiler.
- **HTTP Proxy plutôt que protocole Kafka.** Un client Kafka en JavaScript
  impose une bibliothèque, native ou non maintenue. Le HTTP Proxy de Redpanda
  accepte les mêmes messages par simple requête HTTP.
- **Lecture de SQLite plutôt que de l'API des plugins.** L'API des plugins
  d'OpenCode 2 diffuse des événements mais ne permet pas de lister les
  messages d'une session.
- **Les schémas Avro restent la seule référence.** Le plugin les lit dans
  `schemas/` ; le Schema Registry refuse toute évolution incompatible.

## Prérequis

- OpenCode 2 (testé avec 2.0.17) ;
- la stack Docker démarrée (`docker-compose up -d`), avec le Schema Registry
  (port 8081) et le HTTP Proxy (port 8082) de Redpanda joignables depuis la
  machine où tourne OpenCode, directement ou via `./scripts/tunnel.sh` ;
- Node 22.13 ou plus récent, uniquement pour la ligne de commande.

## Installation

Le plugin s'installe par un **lien symbolique** : le fichier reste dans le
dépôt, et retrouve à partir de son vrai chemin les schémas, le `.env` et le
fichier d'état du projet. Toutes les commandes se lancent **depuis la racine
du dépôt**.

1. Préparer le projet (une seule fois) :

   ```sh
   cp -n .env.example .env        # puis ajuster les URL de Redpanda si besoin
   docker-compose up -d           # Redpanda, sink, Iceberg, RustFS
   ```

2. Vérifier que le plugin trouve la base OpenCode, sans rien envoyer :

   ```sh
   node plugins/opencode/observatory.js --dry-run
   # dry run: pending events -> 12 sessions, 340 messages, …
   ```

3. Créer le lien dans le dossier des plugins d'OpenCode :

   ```sh
   mkdir -p ~/.config/opencode/plugins
   ln -sfn "$PWD/plugins/opencode/observatory.js" ~/.config/opencode/plugins/observatory.js
   ```

4. **Redémarrer OpenCode**, puis contrôler le journal :

   ```sh
   tail -f observatory.log
   # … [observatory] ready for /chemin/du/projet
   # … [observatory] produced to opencode.* -> 0 sessions, 2 messages, 5 parts, 1 tools
   ```

   Le lien doit pointer vers un fichier existant :

   ```sh
   ls -lL ~/.config/opencode/plugins/observatory.js
   ```

La première synchronisation, au démarrage d'OpenCode, publie tout l'historique.
Ensuite, chaque fin de réponse publie le delta.

### Mise à jour

Le lien pointe vers le dépôt : un `git pull` met le plugin à jour. Il suffit
ensuite de redémarrer OpenCode.

### Migration depuis `opencode-plugin/`

Le plugin se trouvait auparavant dans `opencode-plugin/observatory.js`. Après
la mise à jour du dépôt, un lien créé avec l'ancien chemin est **cassé** et
OpenCode ne charge plus le plugin. Pour le vérifier, puis le recréer :

```sh
readlink ~/.config/opencode/plugins/observatory.js   # …/opencode-plugin/observatory.js = ancien chemin
ln -sfn "$PWD/plugins/opencode/observatory.js" ~/.config/opencode/plugins/observatory.js
```

Redémarrez OpenCode. L'état (`data/state/producer-state.json`) est conservé :
rien n'est republié.

## Désinstallation

1. Supprimer le lien (le fichier du dépôt n'est pas touché) :

   ```sh
   rm ~/.config/opencode/plugins/observatory.js
   ```

2. **Redémarrer OpenCode.** La ligne `ready for` ne doit plus apparaître
   dans `observatory.log`.

3. Facultatif, nettoyer le dépôt :

   ```sh
   rm -f data/state/producer-state.json observatory.log
   ```

   Sans état, une réinstallation republie tout l'historique. Les doublons qui
   en résultent sont sans effet : les lecteurs gardent la dernière version de
   chaque ligne.

Les données déjà publiées restent dans Redpanda et Iceberg. La désinstallation
arrête seulement la collecte.

## Configuration

OpenCode ne transmet pas l'environnement du projet aux plugins : le plugin lit
donc le `.env` du dépôt. Une variable d'environnement réelle l'emporte sur le
`.env`.

| Variable | Défaut | Rôle |
| --- | --- | --- |
| `OPENCODE_DB` | `~/.local/share/opencode/opencode.db` | Base source |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry de Redpanda |
| `KAFKA_HTTP_PROXY_URL` | `http://localhost:8082` | HTTP Proxy de Redpanda |
| `KAFKA_TOPIC_PREFIX` | `opencode` | Préfixe des topics |
| `PRODUCER_STATE_FILE` | `data/state/producer-state.json` | Watermarks (relatif au dépôt) |
| `OBSERVATORY_LOG` | `observatory.log` | Journal du plugin (relatif au dépôt) |

## Ligne de commande

```sh
node plugins/opencode/observatory.js             # publier les nouvelles lignes
node plugins/opencode/observatory.js --dry-run   # compter les événements en attente, sans réseau
node plugins/opencode/observatory.js --full      # ignorer le watermark et tout republier
```

`--full` est utile après une remise à zéro de Redpanda ou d'Iceberg. Les
doublons qu'il crée sont sans effet : les lecteurs gardent la dernière version
de chaque identifiant. Node affiche un avertissement `ExperimentalWarning`
pour `node:sqlite`, sans conséquence.

## Dépannage

| Symptôme dans le journal | Cause probable |
| --- | --- |
| Aucune ligne `ready for` | Lien absent ou cassé (`ls -l ~/.config/opencode/plugins/`), ou OpenCode pas redémarré |
| `http://localhost:8082 unreachable` | Stack arrêtée, port 8082 non exposé, ou tunnel fermé |
| `HTTP 409` sur `/subjects/…` | Le schéma modifié n'est pas compatible avec la version enregistrée |
| `batch of … rejected` | Topic absent (relancer `docker-compose up -d redpanda-topics`) ou message trop gros (> 1 Mo) |
| `expected string, got …` | Une donnée OpenCode ne respecte plus le schéma Avro : format de la base changé |

## Limites

- Le plugin dépend du schéma SQLite interne d'OpenCode 2 (`session_v2`,
  `session_message`). Une nouvelle version d'OpenCode peut le modifier.
- Les suppressions de sessions ne sont pas propagées.
- Un message de plus de 1 Mo, par exemple une très grosse sortie d'outil, est
  refusé par Redpanda et bloque la synchronisation jusqu'à correction.
