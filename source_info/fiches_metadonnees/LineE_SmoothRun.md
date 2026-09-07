# Fiche métadonnées — Ligne E (Fonctionnement lisse)

## Identification
- **Identifiant** : LineE
- **Fichier source** : `source/LineE_SmoothRun.csv`
- **Buckets MinIO** : `raw/`, `staging/`, `curated/` (clé `LineE_SmoothRun.csv`)
- **DAG Airflow associé** : `Pipeline_LineE_SmoothRun` (`dags/dag_pipeline.py`)

## Description
Ligne de production la plus régulière du parc : faible variabilité en température
et en pression, aucune anomalie détectée sur la période couverte. Pas de compteur
de temps écoulé disponible.

## Propriétaire
- **Propriétaire métier (ex. responsable maintenance)** : À désigner
- **Propriétaire technique (pipeline)** : Équipe Data Engineering (`owner: data-engineering`, `dags/dag_pipeline.py`)
- **Contact fournisseur des données sources** : Davide Carneiro — davide.r.carneiro@inesctec.pt (INESC TEC / ESTG‑IPP, Portugal)

## Source
- **Origine** : jeu de données "Synthetic Data from Industrial Sensor Monitoring"
  (Carneiro, D., Torres, D., & Peixoto, E., 2025), Zenodo — licence CC BY 4.0
- **Nature** : données de capteurs industriels synthétiques (température, pression)
- **Colonnes** : `timestamp, Temperature, pressure, label`
- **Période couverte** : 01/01/2025 → 04/01/2025
- **Volumétrie** : 5 000 lignes, ~288,2 KB

## Fréquence de collecte
- **Échantillonnage capteur** : 1 mesure / minute
- **Ingestion pipeline (Airflow)** : quotidienne (`schedule="@daily"`)
- **Flux de transformation** : `raw` → `staging` → `curated`
  (`scripts/to_raw.py`, `scripts/to_staging.py`, `scripts/to_curated.py`),
  lineage tracé dans OpenMetadata (service `minio`)

## Caractéristiques / qualité des données
- **Variabilité** : faible (température et pression)
- **Plage température** : ~199 – 200
- **Plage pression** : ~99 – 100
- **Temps écoulé** : non disponible
- **Taux d'anomalies** (`label=1`) : 0 %

## Licence & citation
- **Licence** : CC BY 4.0
- **Citation** : Carneiro, D., Torres, D., & Peixoto, E. (2025). *Synthetic Data from Industrial Sensor Monitoring*. Zenodo.
