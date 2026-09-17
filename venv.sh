#!/bin/sh

VENV="${VENV_PATH:-.venv}"

case "${1:-activate}" in
    activate)
        if [ ! -f "$VENV/bin/activate" ]; then
            printf 'Virtual environment not found. Run: python3 -m venv .venv\n' >&2
            return 1 2>/dev/null || exit 1
        fi
        . "$VENV/bin/activate"
        printf 'Virtual environment activated: %s\n' "$VENV"
        ;;
    deactivate)
        if command -v deactivate >/dev/null 2>&1; then
            deactivate
        else
            printf 'No virtual environment is currently active.\n'
        fi
        ;;
    *)
        printf '%s\n' 'Usage: source ./venv.sh {activate|deactivate}' >&2
        return 1 2>/dev/null || exit 1
        ;;
esac
