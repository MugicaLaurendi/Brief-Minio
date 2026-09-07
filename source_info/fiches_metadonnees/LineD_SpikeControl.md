# Fiche métadonnées — Ligne D (Contrôle avec pics)

## Identification
- **Identifiant** : LineD
- **Fichier source** : `source/LineD_SpikeControl.csv`
- **Buckets MinIO** : `raw/`, `staging/`, `curated/` (clé `LineD_SpikeControl.csv`)
- **DAG Airflow associé** : `Pipeline_LineD_SpikeControl` (`dags/dag_pipeline.py`)

## Description
Ligne de production avec pression maîtrisée mais sujette à des pics de température
(forte variabilité en température, faible variabilité en pression). Pas de compteur
de temps écoulé disponible.

## Propriétaire
- **Propriétaire métier (ex. responsable maintenance)** : À désigner
- **Propriétaire technique (pipeline)** : Équipe Data Engineering (`owner: data-engineering`, `dags/dag_pipeline.py`)
- **Contact fournisseur des données sources** : Davide Carneiro — davide.r.carneiro@inesctec.pt (INESC TEC / ESTG‑IPP, Portugal)

## Source
- **Origine** : jeu de données "Synthetic Data from Industrial Sensor Monitoring"
  (Carneiro, D., Torres, D., & Peixoto, E., 2025), Zenodo — licence CC BY 4.0
- **Nature** : données de capteurs industriels synthétiques (température, pression)
- **Colonnes** : `timestamp, temperature, Pressure, label`
- **Période couverte** : 01/02/2025 → 04/02/2025
- **Volumétrie** : 5 000 lignes, ~288,2 KB

## Fréquence de collecte
- **Échantillonnage capteur** : 1 mesure / minute
- **Ingestion pipeline (Airflow)** : quotidienne (`schedule="@daily"`)
- **Flux de transformation** : `raw` → `staging` → `curated`
  (`scripts/to_raw.py`, `scripts/to_staging.py`, `scripts/to_curated.py`),
  lineage tracé dans OpenMetadata (service `minio`)

## Caractéristiques / qualité des données
- **Variabilité** : élevée (température), faible (pression)
- **Plage température** : ~196 – 202
- **Plage pression** : ~97 – 102
- **Temps écoulé** : non disponible
- **Taux d'anomalies** (`label=1`) : < 5 %

## Licence & citation
- **Licence** : CC BY 4.0
- **Citation** : Carneiro, D., Torres, D., & Peixoto, E. (2025). *Synthetic Data from Industrial Sensor Monitoring*. Zenodo.
