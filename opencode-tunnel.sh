#!/bin/sh
# Ouvre les tunnels SSH vers l'engine podman sur homelab pour le client local.
# Utilisé par app.py (Iceberg REST + S3) et produce.py (Kafka) via .env.

set -eu

HOST="${HOMELAB_HOST:-homelab}"
PORTS="8181 9000 9092"  # iceberg-rest, rustfs (S3), redpanda

for port in $PORTS; do
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then
        printf 'port %s déjà ouvert (tunnel existant ?)\n' "$port"
    else
        printf 'tunnel %s -> %s:%s\n' "$port" "$HOST" "$port"
        ssh -f -N -L "${port}:localhost:${port}" "$HOST"
    fi
done

printf 'Tunnels actifs. Client : streamlit run app.py --server.address=127.0.0.1\n'
