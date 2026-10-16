# Producteur Claude Code

`observatory.js` publie les conversations **Claude Code** dans le même
pipeline que celles d'OpenCode. Il utilise les mêmes topics, les mêmes schémas
Avro et les mêmes tables Iceberg. Chaque ligne porte `source = "claude-code"`,
ce qui permet au dashboard de filtrer par outil.

```text
réponse terminée ─▶ hook Stop ─▶ observatory.js ─▶ Schema Registry + HTTP Proxy
(Claude Code)                        │                          │
               ~/.claude/projects/*.jsonl                   topics opencode.*
                                                                │
                                         sink.py ◀──────────────┘ ─▶ Iceberg / S3
```

## Ce qu'il fait

1. **Lit les transcripts** de Claude Code, en lecture seule. Une session est
   formée du fichier `~/.claude/projects/<projet>/<session>.jsonl` et des
   fichiers de ses sous-agents (`<session>/subagents/agent-*.jsonl`), qui
   partagent le même `sessionId`.
2. **Transforme** chaque session en quatre datasets :

   | Dataset | Contenu |
   | --- | --- |
   | `sessions` | titre (`ai-title`, sinon premier prompt), `cwd`, dernier modèle, totaux de tokens dont le cache, coût estimé |
   | `messages` | prompts utilisateur et réponses (une réponse s'étale sur plusieurs lignes JSONL partageant `message.id`) ; `agent` vaut `main` ou `subagent:<id>` |
   | `parts` | blocs `text`, `thinking` (→ `reasoning`) et `tool_use` (→ `tool`) |
   | `tools` | appel d'outil, complété par son `tool_result` : sortie, statut `completed` / `error` (ou `running` sans résultat) |

3. **Publie** avec l'encodeur Avro et le client Redpanda du plugin OpenCode
   (`publish()` de `plugins/opencode/observatory.js`).
4. **Avance l'état** (`data/state/claude-code-state.json`) seulement une fois
   tous les événements acquittés. Pour chaque session, l'état garde le `mtime`
   de ses fichiers et l'horodatage du dernier événement envoyé. Une session
   dont aucun fichier n'a changé est ignorée. Sinon, elle est relue en entier,
   mais seuls les événements plus récents sont publiés, ainsi que la ligne de
   session qui agrège les totaux.

`output_tokens` inclut déjà le raisonnement : `tokens_reasoning` vaut donc 0,
pour ne pas compter le thinking deux fois.

### Coût estimé

Les transcripts ne contiennent pas de prix, seulement l'usage en tokens. Le
coût est calculé à partir de la grille `PRICES` (en $ par million de tokens :
entrée, sortie, lecture du cache). L'écriture dans le cache vaut 1,25× le prix
d'entrée pour le TTL de 5 minutes, et 2× pour celui d'une heure. Un modèle
absent de la grille est compté à 0 $.

C'est le **tarif API** : avec un abonnement Pro ou Max, ce montant ne
correspond pas à ce qui est facturé, mais il reste un bon ordre de grandeur de
la consommation. Le dashboard l'indique avec la mention « (estimé) ».

## Prérequis

- Claude Code ;
- Node 22 ou plus récent ;
- la stack Docker démarrée (`docker-compose up -d`), avec le Schema Registry
  (8081) et le HTTP Proxy (8082) de Redpanda joignables depuis la machine où
  tourne Claude Code, directement ou via `./scripts/tunnel.sh` ;
- un sink à jour, qui ajoute la colonne `source` aux tables existantes
  (`docker-compose up -d --build sink`) ;
- `jq`, uniquement pour installer le hook en une commande (`brew install jq`).

## Installation

Claude Code n'a pas de dossier de plugins à charger. La publication est
déclenchée par un **hook `Stop`**, que Claude Code exécute à la fin de chaque
réponse. Toutes les commandes se lancent **depuis la racine du dépôt**.

1. Préparer le projet (une seule fois) :

   ```sh
   cp -n .env.example .env        # puis ajuster les URL de Redpanda si besoin
   docker-compose up -d --build   # Redpanda, sink, Iceberg, RustFS
   ```

2. Vérifier que les transcripts sont trouvés, sans rien envoyer :

   ```sh
   node plugins/claude/observatory.js --dry-run
   # dry run: pending events -> 115 sessions, 5035 messages, 9959 parts, 4782 tools
   ```

3. Publier l'historique une première fois. Cela vérifie aussi que Redpanda est
   joignable :

   ```sh
   node plugins/claude/observatory.js
   # produced to opencode.* -> 115 sessions, …
   ```

4. Ajouter le hook à `~/.claude/settings.json`. La commande fait une
   sauvegarde, conserve les autres réglages et hooks, et peut être relancée
   sans créer de doublon :

   ```sh
   SETTINGS=~/.claude/settings.json
   [ -f "$SETTINGS" ] || echo '{}' > "$SETTINGS"
   cp "$SETTINGS" "$SETTINGS.bak"
   CMD="\"$(command -v node)\" \"$PWD/plugins/claude/observatory.js\" --log >/dev/null 2>&1 &"
   jq --arg cmd "$CMD" '.hooks.Stop = ([(.hooks.Stop // [])[]
       | select(any(.hooks[]?; .command | contains("plugins/claude/observatory.js")) | not)]
       + [{"hooks": [{"type": "command", "command": $cmd}]}])' \
     "$SETTINGS.bak" > "$SETTINGS"
   ```

   Le chemin absolu de `node` évite les soucis de `PATH` (nvm, Homebrew) dans
   l'environnement des hooks. Le `&` lance la synchronisation en arrière-plan :
   Claude Code n'attend pas. `--log` écrit le résultat dans le journal
   partagé.

   <details>
   <summary>Installation manuelle, sans jq</summary>

   Ajoutez ce bloc à `~/.claude/settings.json`, en le fusionnant avec un
   éventuel `hooks` existant et en remplaçant les deux chemins :

   ```json
   {
     "hooks": {
       "Stop": [
         {
           "hooks": [
             {
               "type": "command",
               "command": "\"/chemin/vers/node\" \"/chemin/vers/big-data-s3/plugins/claude/observatory.js\" --log >/dev/null 2>&1 &"
             }
           ]
         }
       ]
     }
   }
   ```

   </details>

5. Vérifier : dans Claude Code, `/hooks` doit lister le hook `Stop`. Après
   une réponse, le journal doit afficher une ligne `[claude-code]` :

   ```sh
   tail -f observatory.log
   # … [claude-code] produced to opencode.* -> 1 sessions, 2 messages, 5 parts, 1 tools
   ```

Les sessions déjà ouvertes chargent le hook au prochain démarrage de Claude
Code. Si Redpanda est injoignable, l'erreur est journalisée et l'état n'avance
pas : tout repart à la réponse suivante.

### Mise à jour

Le hook appelle le fichier du dépôt : un `git pull` suffit. Si le dépôt est
déplacé, relancez l'étape 4 depuis le nouvel emplacement : elle remplace
l'ancien hook.

## Désinstallation

1. Retirer le hook de `~/.claude/settings.json`. Les autres hooks et réglages
   sont conservés, et les clés devenues vides sont supprimées :

   ```sh
   SETTINGS=~/.claude/settings.json
   cp "$SETTINGS" "$SETTINGS.bak"
   jq 'if .hooks.Stop then .hooks.Stop |= map(select(any(.hooks[]?;
         .command | contains("plugins/claude/observatory.js")) | not)) else . end
       | if .hooks.Stop == [] then del(.hooks.Stop) else . end
       | if .hooks == {} then del(.hooks) else . end' \
     "$SETTINGS.bak" > "$SETTINGS"
   ```

   Sans jq : ouvrez `~/.claude/settings.json` et supprimez l'entrée de
   `hooks.Stop` dont la commande contient `plugins/claude/observatory.js`.

2. Vérifier : `/hooks` dans Claude Code ne doit plus lister ce hook, et aucune
   nouvelle ligne `[claude-code]` ne doit apparaître dans
   `observatory.log`.

3. Facultatif, nettoyer le dépôt :

   ```sh
   rm -f data/state/claude-code-state.json
   ```

   Sans état, une réinstallation republie tout l'historique. Les doublons qui
   en résultent sont sans effet.

Les données déjà publiées restent dans Redpanda et Iceberg. La désinstallation
arrête seulement la collecte. Les transcripts de `~/.claude/projects` ne sont
jamais modifiés.

## Ligne de commande

```sh
node plugins/claude/observatory.js             # publier les nouveautés
node plugins/claude/observatory.js --dry-run   # compter les événements en attente, sans réseau
node plugins/claude/observatory.js --full      # ignorer l'état et tout republier
node plugins/claude/observatory.js --log       # écrire le résultat dans le journal (mode hook)
```

## Dépannage

| Symptôme | Cause probable |
| --- | --- |
| Aucune ligne `[claude-code]` dans le journal | Hook absent (`/hooks`), Claude Code pas redémarré, ou chemin de `node` / du dépôt invalide : lancez la commande du hook à la main sans `>/dev/null` |
| `http://localhost:8082 unreachable` | Stack arrêtée, port 8082 non exposé, ou tunnel fermé |
| `Claude Code projects not found` | `CLAUDE_PROJECTS_DIR` erroné |
| `HTTP 409` sur `/subjects/…` | Schéma Avro incompatible avec la version enregistrée |
| Pas de filtre Claude Code dans le dashboard | Sink pas reconstruit : la colonne `source` manque |

## Configuration

Les variables sont lues dans le `.env` du dépôt. Une variable d'environnement
réelle l'emporte.

| Variable | Défaut | Rôle |
| --- | --- | --- |
| `CLAUDE_PROJECTS_DIR` | `~/.claude/projects` | Transcripts sources |
| `CLAUDE_STATE_FILE` | `data/state/claude-code-state.json` | État incrémental (relatif au dépôt) |
| `SCHEMA_REGISTRY_URL`, `KAFKA_HTTP_PROXY_URL`, `KAFKA_TOPIC_PREFIX`, `OBSERVATORY_LOG` | voir [le plugin OpenCode](../opencode/README.md#configuration) | Partagées |

## Confidentialité

Les transcripts contiennent les prompts, le code lu, les commandes et les
sorties d'outils, parfois des secrets affichés dans un terminal. Ils
aboutissent dans le warehouse S3 et dans le dashboard : gardez ces services
sur un réseau de confiance.

## Limites

- Le format des transcripts est interne à Claude Code et peut changer.
- Les sessions supprimées ne sont pas propagées.
- Comme pour OpenCode, un événement de plus de 1 Mo (très grosse sortie
  d'outil) est refusé par Redpanda et bloque la synchronisation.
