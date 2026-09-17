# OpenCode Observatory

Pipeline local d'analyse des conversations OpenCode :

`opencode.db` -> Parquet -> RustFS/S3 -> DuckDB -> Streamlit

Les données peuvent contenir des prompts, chemins, commandes et sorties d'outils sensibles. Ne rendez pas le bucket public.

## Installation

```sh
python3 -m venv .venv
. ./venv.sh activate
pip install -r requirements.txt
cp .env.example .env
```

Modifiez `.env` si nécessaire. Le fichier `.env` ne doit jamais être commité.

## Démarrage local

```sh
docker compose up -d rustfs
set -a; . ./.env; set +a
./opencode-analytics.sh sync
streamlit run app.py --server.address=127.0.0.1
```

Dashboard : <http://localhost:8501>

Pour synchroniser automatiquement toutes les 30 secondes :

```sh
./opencode-analytics.sh enable
./opencode-analytics.sh status
./opencode-analytics.sh disable
```

Le venv peut être désactivé avec :

```sh
. ./venv.sh deactivate
```

## Dashboard Docker

Le dashboard peut aussi récupérer les Parquet depuis RustFS :

```sh
docker compose --profile dashboard up --build dashboard
```

Le service est exposé sur `127.0.0.1:8501`. Pour un serveur distant, utilisez un tunnel SSH :

```sh
ssh -N -L 8501:127.0.0.1:8501 utilisateur@serveur
```
