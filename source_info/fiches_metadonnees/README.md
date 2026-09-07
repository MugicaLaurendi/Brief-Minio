# Fiches métadonnées — Lignes de production

Cinq fiches, une par ligne de production, décrivant description, propriétaire,
source et fréquence de collecte. Basées sur les fichiers de `source/`, les scripts
d'ingestion (`dags/dag_pipeline.py`, `dags/scripts/`) et la documentation du jeu
de données (`source_info/`).

| Ligne | Fiche | Caractéristique | Anomalies | Période |
|---|---|---|---|---|
| Ligne A | [LineA_Stable_10K.md](LineA_Stable_10K.md) | Stable, gros volume (10K) | < 1 % | 05/2025 |
| Ligne B | [LineB_Flux.md](LineB_Flux.md) | Variabilité moyenne | < 1 % | 04/2025 |
| Ligne C | [LineC_Turbulent.md](LineC_Turbulent.md) | Turbulente, forte variabilité temp. | < 5 % | 03/2025 |
| Ligne D | [LineD_SpikeControl.md](LineD_SpikeControl.md) | Pression maîtrisée, pics de temp. | < 5 % | 02/2025 |
| Ligne E | [LineE_SmoothRun.md](LineE_SmoothRun.md) | Fonctionnement lisse | 0 % | 01/2025 |

## Notes communes à toutes les lignes

- **Source** : jeu de données synthétique "Synthetic Data from Industrial Sensor
  Monitoring" (Carneiro, Torres & Peixoto, 2025, Zenodo, CC BY 4.0), contact
  Davide Carneiro (davide.r.carneiro@inesctec.pt, INESC TEC / ESTG‑IPP).
- **Fréquence de collecte capteur** : 1 mesure/minute (résolution des `timestamp`).
- **Fréquence d'ingestion pipeline** : quotidienne (Airflow, `schedule="@daily"`),
  flux `raw → staging → curated` dans MinIO, lineage suivi dans OpenMetadata.
- **Propriétaire métier** : non renseigné dans les données source — champ
  "À désigner" à compléter par le responsable maintenance réel de chaque ligne.
- **Propriétaire technique** : équipe Data Engineering (`owner: data-engineering`
  dans `dags/dag_pipeline.py`).
