#!/bin/sh
# Publie les nouvelles lignes d'OpenCode dans Redpanda (une exécution).
# Lancé par le plugin opencode-plugin/observatory.js après chaque réponse, et
# par cron en rattrapage. Cron ne charge pas l'environnement du shell : le .env
# du projet est donc lu ici. Exemple de crontab (toutes les heures) :
#   0 * * * * /chemin/vers/big-data-s3/opencode-analytics.sh >> /chemin/vers/big-data-s3/opencode-analytics.log 2>&1

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$ROOT/.env" ]; then
    set -a
    . "$ROOT/.env"
    set +a
fi
if [ -z "${PYTHON:-}" ]; then
    if [ -x "$ROOT/.venv/bin/python" ]; then PYTHON="$ROOT/.venv/bin/python"; else PYTHON=python3; fi
fi

cd "$ROOT"
printf '%s ' "$(date '+%Y-%m-%d %H:%M:%S')"
exec "$PYTHON" produce.py \
    --database "${OPENCODE_DB:-$HOME/.local/share/opencode/opencode.db}" \
    --state "${PRODUCER_STATE_FILE:-$ROOT/data/state/producer-state.json}" \
    "$@"
