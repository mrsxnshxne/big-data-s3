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
   (`publish()` de `opencode-plugin/observatory.js`).
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

- Node 22 ou plus récent ;
- la stack Docker démarrée, avec le Schema Registry (8081) et le HTTP Proxy
  (8082) de Redpanda joignables (au besoin via `./opencode-tunnel.sh`) ;
- le sink à jour : il ajoute la colonne `source` aux tables existantes.

## Première publication

```sh
node claude-code-plugin/observatory.js --dry-run   # compter, sans réseau
node claude-code-plugin/observatory.js             # publier tout l'historique
```

## Publication automatique (hook Stop)

Ajoutez un hook `Stop` à `~/.claude/settings.json`, en remplaçant le chemin
par celui du dépôt. Le `&` lance la synchronisation en arrière-plan pour ne
pas ralentir Claude Code. `--log` écrit le résultat dans le journal partagé
(`opencode-analytics.log`).

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "node /chemin/vers/big-data-s3/claude-code-plugin/observatory.js --log >/dev/null 2>&1 &"
          }
        ]
      }
    ]
  }
}
```

```sh
tail -f opencode-analytics.log
# … [claude-code] produced to opencode.* -> 1 sessions, 2 messages, 5 parts, 1 tools
```

Si Redpanda est injoignable, l'erreur est journalisée et l'état n'avance pas :
tout repart à la réponse suivante.

## Désinstallation

Retirez le hook de `~/.claude/settings.json`. L'état
`data/state/claude-code-state.json` peut être supprimé sans risque ; sans lui,
la prochaine exécution republie tout l'historique.

## Configuration

Les variables sont lues dans le `.env` du dépôt. Une variable d'environnement
réelle l'emporte.

| Variable | Défaut | Rôle |
| --- | --- | --- |
| `CLAUDE_PROJECTS_DIR` | `~/.claude/projects` | Transcripts sources |
| `CLAUDE_STATE_FILE` | `data/state/claude-code-state.json` | État incrémental (relatif au dépôt) |
| `SCHEMA_REGISTRY_URL`, `KAFKA_HTTP_PROXY_URL`, `KAFKA_TOPIC_PREFIX`, `OBSERVATORY_LOG` | voir [le plugin OpenCode](../opencode-plugin/README.md#configuration) | Partagées |

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
