# Temps réel : Arroyo et panel SSE

Le chemin « historique » (`sink.py` → Iceberg → Streamlit) fonctionne par lots :
quelques secondes pour l'écriture, 30 s de cache côté dashboard et un
rechargement manuel. Le chemin temps réel est parallèle et ne passe pas par S3.

```text
Redpanda opencode.* (Avro)
   |
   +--> sink.py ------> Iceberg / RustFS (historique, inchangé)
   |
   +--> Arroyo (arroyo/live.sql)
           |  JSON plat, sans prompt ni sortie d'outil
           v
        Redpanda live.events / live.activity
           |
           v
        live/server.py  (FastAPI, consommateur Kafka)
           |  Server-Sent Events  /stream
           v
        panel HTML/JS  live/static/index.html   http://localhost:8600
```

Les deux chemins lisent les mêmes topics avec des groupes de consommateurs
distincts : ajouter le temps réel ne modifie ni le producteur ni les tables.

## Composants

| Fichier | Rôle |
| --- | --- |
| `arroyo/live.sql` | Pipeline Arroyo : lit `opencode.messages/tools/sessions` (Avro + Schema Registry), écrit `live.events` (un événement plat par ligne) et `live.activity` (fenêtres de 10 s par modèle) |
| `arroyo/deploy.py` | Enregistre chaque `arroyo/*.sql` comme pipeline via l'API REST d'Arroyo ; `--replace` redéploie |
| `live/server.py` | Pont SSE : rejoue `live.*` depuis le début pour reconstruire les totaux, puis suit les nouveaux événements |
| `live/static/index.html` | Panel sans dépendance : totaux, flux d'événements, coût par modèle, outils, activité |

Services Compose : `arroyo` (moteur, UI sur <http://localhost:5115>),
`arroyo-deploy` (one-shot qui déploie les pipelines) et `live` (pont + panel).

## Protocole SSE

`GET /stream` envoie :

- `snapshot` à la connexion : totaux, derniers événements, titres de sessions ;
- `event` pour chaque message ou appel d'outil récent ;
- `session` quand un titre de session change ;
- `stats` au plus une fois par seconde quand quelque chose a changé.

Un commentaire `: keepalive` part chaque seconde sans changement. `EventSource`
se reconnecte seul et reçoit un nouveau `snapshot`.

## Démarrage

```sh
docker-compose up -d redpanda redpanda-topics rustfs iceberg-rest sink arroyo live
docker-compose run --rm arroyo-deploy        # ou: up -d arroyo-deploy
```

Ouvrir <http://localhost:8600>. Après modification de `arroyo/live.sql` :

```sh
docker-compose run --rm arroyo-deploy python arroyo/deploy.py --replace
```

Redéployer rejoue tout l'historique dans `live.events` (doublons dédupliqués par
identifiant côté pont). Pour repartir d'un topic vide, le supprimer avant :
`rpk topic delete live.events live.activity`, puis relancer `redpanda-topics`.

## Accès sans tunnel

`BIND_ADDRESS=0.0.0.0` dans le `.env` de la machine qui héberge Compose publie
tous les ports (Kafka 9092, Schema Registry 8081, HTTP Proxy 8082, S3 9000/9001,
Iceberg 8181, Arroyo 5115, panel 8600, dashboard 8501) sur toutes ses interfaces.
`KAFKA_ADVERTISED_HOST` doit alors être le nom sous lequel les clients joignent
l'hôte (ex. `homelab`), car c'est l'adresse que Kafka leur annonce.

Côté client, il suffit de pointer le `.env` vers l'hôte au lieu de `localhost`
(`SCHEMA_REGISTRY_URL`, `KAFKA_HTTP_PROXY_URL`, `ICEBERG_CATALOG_URI`,
`ICEBERG_S3_ENDPOINT`, `S3_ENDPOINT`). `scripts/tunnel.sh` n'est alors plus
nécessaire.

Aucun de ces services n'a d'authentification : à réserver à un réseau de
confiance (Tailscale ici). Ne pas le faire sur un réseau partagé.

## Latence et limites

- Le producteur publie à la **fin de chaque réponse** (hook `Stop` / événement
  `session.execution.*`) : le panel est temps réel à partir de Redpanda, pas
  pendant qu'un agent travaille. Publier plus finement demande de changer les
  producteurs.
- Le délai Redpanda → navigateur est de l'ordre de la seconde.
- Seuls les événements de moins de 10 minutes sont animés dans le flux ; les
  plus anciens (rejeu de l'historique) alimentent uniquement les totaux.
- Les totaux sont reconstruits au démarrage du pont en relisant `live.events`
  (rétention illimitée : la rétention Redpanda se calcule sur la date d'origine des événements rejoués, une rétention de 24 h viderait le topic) : un redémarrage les recalcule, ils ne sont pas persistés.
- Le panel n'a pas d'authentification : `LIVE_BIND=127.0.0.1` par défaut.

## Particularités d'Arroyo 0.15

- Le décodeur Avro lit les `timestamp-micros` comme des nanosecondes (les dates
  tombent en 1970). La valeur brute est la bonne en microsecondes, donc
  `live.sql` la reconvertit avec `to_timestamp_micros(CAST(col AS BIGINT))`.
- Le sink Iceberg d'Arroyo exige un catalogue qui répond à
  `HEAD /v1/namespaces/{ns}`, ce que fait `apache/iceberg-rest-fixture` mais
  pas `tabulario/iceberg-rest:1.6.0`. Il n'est pas utilisé ici : l'historique
  Iceberg reste écrit par `sink.py`.
