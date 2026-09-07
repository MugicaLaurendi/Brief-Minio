# Fiche métadonnées — Ligne A (Stable)

## Identification
- **Identifiant** : LineA
- **Fichier source** : `source/LineA_Stable_10K.csv`
- **Buckets MinIO** : `raw/`, `staging/`, `curated/` (clé `LineA_Stable_10K.csv`)
- **DAG Airflow associé** : `Pipeline_LineA_Stable_10K` (`dags/dag_pipeline.py`)

## Description
Ligne de production la plus stable du parc. Capteurs de température et de pression
avec faible variabilité, complétés par un compteur de temps écoulé (`elapsed_time`).
C'est le plus grand des cinq jeux de données (10 000 enregistrements).

## Propriétaire
- **Propriétaire métier (ex. responsable maintenance)** : À désigner
- **Propriétaire technique (pipeline)** : Équipe Data Engineering (`owner: data-engineering`, `dags/dag_pipeline.py`)
- **Contact fournisseur des données sources** : Davide Carneiro — davide.r.carneiro@inesctec.pt (INESC TEC / ESTG‑IPP, Portugal)

## Source
- **Origine** : jeu de données "Synthetic Data from Industrial Sensor Monitoring"
  (Carneiro, D., Torres, D., & Peixoto, E., 2025), Zenodo — licence CC BY 4.0
- **Nature** : données de capteurs industriels synthétiques (température, pression, temps écoulé)
- **Colonnes** : `timestamp, Temperature, pressure, elapsed_time, label`
- **Période couverte** : 01/05/2025 → 07/05/2025
- **Volumétrie** : 10 000 lignes, ~757,5 KB

## Fréquence de collecte
- **Échantillonnage capteur** : 1 mesure / minute
- **Ingestion pipeline (Airflow)** : quotidienne (`schedule="@daily"`)
- **Flux de transformation** : `raw` → `staging` → `curated`
  (`scripts/to_raw.py`, `scripts/to_staging.py`, `scripts/to_curated.py`),
  lineage tracé dans OpenMetadata (service `minio`)

## Caractéristiques / qualité des données
- **Variabilité** : faible (température et pression)
- **Plage température** : ~179 – 180
- **Plage pression** : ~159 – 160
- **Plage temps écoulé** : ~34 – 35
- **Taux d'anomalies** (`label=1`) : < 1 %

## Licence & citation
- **Licence** : CC BY 4.0
- **Citation** : Carneiro, D., Torres, D., & Peixoto, E. (2025). *Synthetic Data from Industrial Sensor Monitoring*. Zenodo.
