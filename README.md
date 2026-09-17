# RustFS S3

## Démarrer RustFS

Configurer les identifiants si nécessaire, puis démarrer le service :

```sh
export S3_ACCESS_KEY=rustfsadmin
export S3_SECRET_KEY=rustfsadmin
docker compose up -d rustfs
```

- API S3 : `http://localhost:9000`
- Console : `http://localhost:9001`

## Tester l’accès S3

Le script crée un bucket, écrit un objet, le relit et compare son contenu.
AWS CLI est requis localement :

```sh
sudo apt update && sudo apt install -y awscli
chmod +x test-s3.sh
```

Test local :

```sh
./test-s3.sh
```

Test d’un RustFS distant :

```sh
export S3_ENDPOINT=http://ADRESSE_DU_VPS:9000
export S3_ACCESS_KEY=identifiant-rustfs
export S3_SECRET_KEY=secret-rustfs
./test-s3.sh
```

Le port `9000` est celui de l’API S3. Le port `9001` est réservé à la console.
