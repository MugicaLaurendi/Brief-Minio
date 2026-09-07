# Fiche métadonnées — Ligne C (Turbulente)

## Identification
- **Identifiant** : LineC
- **Fichier source** : `source/LineC_Turbulent.csv`
- **Buckets MinIO** : `raw/`, `staging/`, `curated/` (clé `LineC_Turbulent.csv`)
- **DAG Airflow associé** : `Pipeline_LineC_Turbulent` (`dags/dag_pipeline.py`)

## Description
Ligne de production turbulente présentant une forte variabilité de température et
une variabilité moyenne de pression. Pas de compteur de temps écoulé disponible.
Taux d'anomalies plus élevé que les lignes A/B.

## Propriétaire
- **Propriétaire métier (ex. responsable maintenance)** : À désigner
- **Propriétaire technique (pipeline)** : Équipe Data Engineering (`owner: data-engineering`, `dags/dag_pipeline.py`)
- **Contact fournisseur des données sources** : Davide Carneiro — davide.r.carneiro@inesctec.pt (INESC TEC / ESTG‑IPP, Portugal)

## Source
- **Origine** : jeu de données "Synthetic Data from Industrial Sensor Monitoring"
  (Carneiro, D., Torres, D., & Peixoto, E., 2025), Zenodo — licence CC BY 4.0
- **Nature** : données de capteurs industriels synthétiques (température, pression)
- **Colonnes** : `timestamp, Temperature, pressure, label`
- **Période couverte** : 01/03/2025 → 04/03/2025
- **Volumétrie** : 5 000 lignes, ~288,1 KB

## Fréquence de collecte
- **Échantillonnage capteur** : 1 mesure / minute
- **Ingestion pipeline (Airflow)** : quotidienne (`schedule="@daily"`)
- **Flux de transformation** : `raw` → `staging` → `curated`
  (`scripts/to_raw.py`, `scripts/to_staging.py`, `scripts/to_curated.py`),
  lineage tracé dans OpenMetadata (service `minio`)

## Caractéristiques / qualité des données
- **Variabilité** : élevée (température), moyenne (pression)
- **Plage température** : ~196 – 210
- **Plage pression** : ~97 – 103
- **Temps écoulé** : non disponible
- **Taux d'anomalies** (`label=1`) : < 5 %

## Licence & citation
- **Licence** : CC BY 4.0
- **Citation** : Carneiro, D., Torres, D., & Peixoto, E. (2025). *Synthetic Data from Industrial Sensor Monitoring*. Zenodo.
