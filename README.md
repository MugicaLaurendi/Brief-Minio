# Projet MinIO Data Lake — Résumé technique

Data lake local (Docker Compose) organisé en couches **raw → staging → curated → archive**
sur **MinIO**, orchestré par **Airflow**, avec catalogage / lineage dans **OpenMetadata**.
Le jeu de données source est un jeu synthétique de capteurs industriels (5 "lignes de
production" : `LineA_Stable_10K`, `LineB_Flux`, `LineC_Turbulent`, `LineD_SpikeControl`,
`LineE_SmoothRun`), documenté dans [source_info/fiches_metadonnees/](source_info/fiches_metadonnees/).

## Architecture générale

```
source/*.csv ──▶ [Airflow: dag_pipeline]──▶ MinIO raw ──▶ staging ──▶ curated ──▶ (180j) archive
                          │                                 │            │
                          └────────────── lineage ──────────┴────────────┘──▶ OpenMetadata
```

Services Docker Compose ([docker-compose.yml](docker-compose.yml)) :

| Service | Rôle |
|---|---|
| `minio` | Stockage objet S3, buckets `raw` / `staging` / `curated` / `archive` |
| `createbuckets` | Conteneur `mc` jetable : crée les 4 buckets et active le chiffrement SSE-S3 au démarrage |
| `kes` | Key Encryption Service, fournit les clés à MinIO pour SSE-S3 |
| `mysql` | Base partagée : schémas `openmetadata_db` et `airflow_db` |
| `elasticsearch` | Index de recherche d'OpenMetadata |
| `openmetadata-server` | Catalogue de données, lineage, UI |
| `execute-migrate-all` | Job one-shot de migration du schéma OpenMetadata |
| `ingestion` | Airflow (image officielle `openmetadata/ingestion`), exécuteur `LocalExecutor` |

---

## MinIO — buckets et flux de données

4 buckets, un par étape du pipeline :

- **raw** : dépôt brut des CSV sources, tels qu'uploadés depuis `source/`.
- **staging** : première transformation (harmonisation légère).
- **curated** : données prêtes à consommer (transformation actuellement en placeholder, voir plus bas).
- **archive** : objets `curated` de plus de 180 jours, déplacés par le DAG de maintenance.

Tous les buckets partagent un préfixe commun `lines/` ([dags/scripts/minio_connexion.py](dags/scripts/minio_connexion.py))
avant le nom de fichier (ex. `lines/LineA_Stable_10K.csv`). Ce préfixe est un
**contournement volontaire** : OpenMetadata ne peut extraire le schéma d'un fichier via son
manifest de conteneur que si celui-ci vit dans un "dossier" plutôt qu'à la racine du bucket
(cf. section OpenMetadata ci-dessous).

> **Partitionnement year/month/line** : évoqué comme cible d'architecture, mais **non
> implémenté actuellement**. La clé objet réelle est `lines/<nom_fichier>.csv` — un seul
> objet par ligne de production, réécrit à chaque exécution du DAG (pas de découpage
> temporel par année/mois). C'est une piste d'évolution si l'historisation par run devient
> nécessaire.

### Upload par chunks

[dags/scripts/to_raw.py](dags/scripts/to_raw.py) utilise `boto3.s3.transfer.TransferConfig`
pour forcer un upload multipart dès que le fichier dépasse une taille de part
(`MINIO_UPLOAD_CHUNK_SIZE`, 8 Mo par défaut, minimum S3 = 5 Mo) — `multipart_threshold`
et `multipart_chunksize` sont réglés à la même valeur pour découper systématiquement en
chunks plutôt que d'attendre un gros seuil.

### Archivage à 180 jours

[dags/dag_archivage.py](dags/dag_archivage.py) (`minio_archive_180d`, `@daily`) :

- Liste les objets de `curated` (pagination `list_objects_v2`).
- Pour chaque objet dont `LastModified` dépasse `MINIO_ARCHIVE_RETENTION_DAYS` (180j par
  défaut), copie vers `archive` puis suppression de la source.
- **L'archivage porte sur les fichiers/objets entiers**, pas sur les lignes à l'intérieur
  d'un CSV — un objet est archivé en bloc dès qu'il a dépassé l'âge de rétention.
- Déclenche un enregistrement de lineage `curated → archive` dans OpenMetadata si au moins
  un objet a été déplacé.

### Comptes de service et policies IAM

[minio-iam/setup-service-accounts.sh](minio-iam/setup-service-accounts.sh) crée 3 policies
et 3 comptes de service MinIO (access/secret key dédiées, pas de mot de passe interactif) :

| Compte | Policy | Droits |
|---|---|---|
| `svc-data-analyst` | [data-analyst-readonly.json](minio-iam/policies/data-analyst-readonly.json) | Lecture seule sur `curated/` |
| `svc-data-engineer` | [data-engineer-readwrite.json](minio-iam/policies/data-engineer-readwrite.json) | Lecture/écriture sur `raw/`, `staging/`, `curated/` |
| `svc-admin` | — | Hérite des pleins droits du compte root (pas de policy restrictive) |

Le script est ré-exécutable sans risque (les policies sont recréées à l'identique, la
création d'un compte déjà existant échoue proprement sans rien casser). Les secrets générés
sont écrits dans `minio-iam/credentials.generated.env` (chmod 600, exclu du dépôt).

### Chiffrement des données

- **Chiffrement au repos activé** : chaque bucket est passé en SSE-S3
  (`mc encrypt set sse-s3 …`) au moment de sa création par le conteneur `createbuckets`.
- Les clés sont fournies par **KES** (Key Encryption Service), configuré en `fs` keystore
  local ([kes/config.yml](kes/config.yml) — explicitement noté "fine for local/dev, not for
  production").
- **KES est déprécié côté MinIO** (plus maintenu, remplacé par MinKMS pour la gamme AIStor)
  mais reste fonctionnel pour un usage local. **Point à traiter** : migrer vers une
  alternative maintenue avant tout usage au-delà du dev.
- **Pas de chiffrement des flux de données côté Airflow** : les échanges entre les tâches
  Airflow et MinIO (SDK `boto3`) ne sont pas chiffrés en transit dans la configuration
  actuelle (`MINIO_SECURE=false`, endpoint en `http://`) — seul le stockage au repos dans
  MinIO est couvert.

---

## Airflow — DAGs

### DAG d'ingestion / pipeline (`dag_pipeline.py`)

Un DAG généré **dynamiquement par fichier CSV** trouvé dans `source/` (`Pipeline_<nom_du_fichier>`,
`@daily`, pas de catchup). Trois tâches séquentielles :

1. **`import_to_raw`** — upload du CSV source vers le bucket `raw` (upload chunké, cf. ci-dessus).
2. **`transform_to_staging`** ([to_staging.py](dags/scripts/to_staging.py)) — télécharge depuis
   `raw`, harmonise les noms de colonnes (`df.columns.str.lower()`), réupload vers `staging`.
   Enregistre le lineage `raw → staging` dans OpenMetadata.
3. **`transform_to_curated`** ([to_curated.py](dags/scripts/to_curated.py)) — télécharge depuis
   `staging`, réupload vers `curated`. **La logique de transformation est actuellement un
   placeholder** (`# faire les transformations` — aucune opération réelle n'est effectuée :
   pas encore de normalisation des formats de timestamp ni d'autres règles métier).
   Le lineage `staging → curated` est bien enregistré même si la transformation est vide.

> **État réel vs. cible** : l'harmonisation des noms de colonnes est partielle (mise en
> minuscule uniquement, pas de mapping vers un schéma canonique inter-lignes) et la
> normalisation des timestamps n'est pas encore implémentée — les deux sont à construire
> dans `to_staging.py` / `to_curated.py`.

### DAG de maintenance (`dag_archivage.py`)

Voir section Archivage ci-dessus (`minio_archive_180d`).

### DAGs générés par OpenMetadata

`airflow_local_metadata.py` et `minio_metadata_extraction.py` sont des DAGs **auto-générés**
par OpenMetadata (`workflow_factory.WorkflowFactory`, à partir des fichiers JSON dans
[dag_generated_configs/](dag_generated_configs/)) — l'un scanne les métadonnées Airflow lui-même
comme source, l'autre scanne le service de stockage `minio` (extraction des schémas de
conteneurs). Ne pas éditer ces fichiers à la main : ils sont régénérés par OpenMetadata.

---

## OpenMetadata

Catalogue de données connecté à MinIO comme **storage service**, avec ingestion
programmée quotidiennement (`minio_metadata_extraction`, cron `0 0 * * *`).

### Difficultés rencontrées

- **Stabilité Docker** : mise en place longue et instable, avec des crashs liés à
  **deux instances Airflow tournant simultanément** (le service `ingestion` du
  docker-compose du projet + une instance Airflow gérée par OpenMetadata elle-même via
  `PIPELINE_SERVICE_CLIENT` pointant vers `http://ingestion:8080`) — source de conflits
  d'état et de redémarrages.
- **Documentation des colonnes / extraction de schéma** : OpenMetadata ne peut extraire le
  schéma d'un fichier via son manifest que si celui-ci se trouve dans un **sous-dossier**
  du bucket, pas à la racine. C'est la raison d'être du préfixe `lines/` appliqué
  uniformément dans `minio_connexion.py` (cf. section MinIO) — un contournement plutôt
  qu'une conception initiale.

### Lineage

- Le lineage est enregistré au niveau **conteneur à conteneur** (bucket → bucket), avec un
  rattachement optionnel au Pipeline Airflow qui a effectué la transformation
  ([dags/scripts/om_lineage.py](dags/scripts/om_lineage.py), fonction `link_buckets`) :
  `raw → staging`, `staging → curated`, et `curated → archive` lors de l'archivage.
- **Limite actuelle** : le lineage est tracé à la granularité du bucket entier, pas au
  niveau du fichier/objet individuel ni des colonnes (pas de lineage niveau colonne).
  Amélioration possible : lineage plus fin par fichier ou par ligne de production.

---

## Résumé des points d'attention connus

| Sujet | État |
|---|---|
| Partitionnement year/month/line | Non implémenté (préfixe `lines/` seulement) |
| Harmonisation des noms de colonnes | Partielle (lowercase uniquement, pas de mapping canonique) |
| Normalisation des formats de timestamp | Non implémentée (`to_curated.py` est un placeholder) |
| Stabilité OpenMetadata/Airflow en local | Historique de crashs liés à deux instances Airflow concurrentes |
| Extraction de schéma OpenMetadata | Nécessite un sous-dossier dans le bucket (contourné via préfixe `lines/`) |
| Lineage | Fonctionnel au niveau bucket, granularité fichier/colonne à améliorer |
| Archivage 180 jours | Implémenté, au niveau fichier/objet (pas ligne à ligne) |
| Chiffrement au repos (buckets) | Activé (SSE-S3 via KES) |
| Chiffrement en transit (Airflow ↔ MinIO) | Non activé (`MINIO_SECURE=false`) |
| KES | Fonctionnel mais déprécié côté MinIO — à remplacer |
| Comptes de service / policies IAM | Implémentés (analyst read-only, engineer read-write, admin) |
