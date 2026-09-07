# Fiche métadonnées — Ligne B (Flux)

## Identification
- **Identifiant** : LineB
- **Fichier source** : `source/LineB_Flux.csv`
- **Buckets MinIO** : `raw/`, `staging/`, `curated/` (clé `LineB_Flux.csv`)
- **DAG Airflow associé** : `Pipeline_LineB_Flux` (`dags/dag_pipeline.py`)

## Description
Ligne de production à variabilité modérée en température et en pression, avec un
compteur de temps écoulé (`Elapsed_time`). Représente des fluctuations de fonctionnement
courantes, sans dérive majeure.

## Propriétaire
- **Propriétaire métier (ex. responsable maintenance)** : À désigner
- **Propriétaire technique (pipeline)** : Équipe Data Engineering (`owner: data-engineering`, `dags/dag_pipeline.py`)
- **Contact fournisseur des données sources** : Davide Carneiro — davide.r.carneiro@inesctec.pt (INESC TEC / ESTG‑IPP, Portugal)

## Source
- **Origine** : jeu de données "Synthetic Data from Industrial Sensor Monitoring"
  (Carneiro, D., Torres, D., & Peixoto, E., 2025), Zenodo — licence CC BY 4.0
- **Nature** : données de capteurs industriels synthétiques (température, pression, temps écoulé)
- **Colonnes** : `timestamp, temperature, pressure, Elapsed_time, label`
- **Période couverte** : 01/04/2025 → 04/04/2025
- **Volumétrie** : 5 000 lignes, ~381,6 KB

## Fréquence de collecte
- **Échantillonnage capteur** : 1 mesure / minute
- **Ingestion pipeline (Airflow)** : quotidienne (`schedule="@daily"`)
- **Flux de transformation** : `raw` → `staging` → `curated`
  (`scripts/to_raw.py`, `scripts/to_staging.py`, `scripts/to_curated.py`),
  lineage tracé dans OpenMetadata (service `minio`)

## Caractéristiques / qualité des données
- **Variabilité** : moyenne (température et pression)
- **Plage température** : ~188 – 191
- **Plage pression** : ~118 – 122
- **Plage temps écoulé** : ~19 – 20
- **Taux d'anomalies** (`label=1`) : < 1 %

## Licence & citation
- **Licence** : CC BY 4.0
- **Citation** : Carneiro, D., Torres, D., & Peixoto, E. (2025). *Synthetic Data from Industrial Sensor Monitoring*. Zenodo.
