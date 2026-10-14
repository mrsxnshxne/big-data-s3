# Plugins de collecte

Chaque outil a son producteur. Les deux publient dans les mêmes topics
Redpanda (`opencode.*`), avec les mêmes schémas Avro, et alimentent les mêmes
tables Iceberg. La colonne `source` indique l'outil d'origine.

| Dossier | Outil | Source lue | Déclenchement | Guide |
| --- | --- | --- | --- | --- |
| [`opencode/`](opencode/) | OpenCode 2 | `~/.local/share/opencode/opencode.db` | plugin chargé par OpenCode (fin de réponse et démarrage) | [Installation / désinstallation](opencode/README.md#installation) |
| [`claude/`](claude/) | Claude Code | `~/.claude/projects/**/*.jsonl` | hook `Stop` de Claude Code (fin de réponse) | [Installation / désinstallation](claude/README.md#installation) |

Les deux s'installent et se désinstallent indépendamment. Les commandes des
guides se lancent depuis la racine du dépôt.

Le producteur Claude Code réutilise l'encodeur Avro et le client Redpanda de
`opencode/observatory.js` : déplacez ou supprimez les deux dossiers ensemble.

> Ancienne arborescence : les plugins étaient dans `opencode-plugin/` et
> `claude-code-plugin/`. Un lien OpenCode créé avec l'ancien chemin est cassé :
> voir [la migration](opencode/README.md#migration-depuis-opencode-plugin).
