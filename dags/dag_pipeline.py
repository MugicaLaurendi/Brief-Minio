from __future__ import annotations
import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator # type: ignore
from airflow.hooks.base import BaseHook
from dotenv import load_dotenv

from scripts.to_raw import get_minio_client ,upload_file
from scripts.to_staging import transformation_to_staging
from scripts.to_curated import transformation_to_curated


load_dotenv()

CSV_FOLDER    = Path("/opt/airflow/source")
MINIO_BUCKET_RAW  = os.getenv("MINIO_BUCKET_RAW", "raw")
MINIO_BUCKET_STAGING  = os.getenv("MINIO_BUCKET_STAGING", "staging")
MINIO_BUCKET_CURATED  = os.getenv("MINIO_BUCKET_CURATED", "curated")


s3 = get_minio_client()


# ----------- DAG ------------

for file in CSV_FOLDER.glob("*.csv"):

    print(f"Found CSV: {file.name}")  # Debug : liste les fichiers trouvés

    default_args = {
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "email_on_failure": False,
    }

    with DAG(
        dag_id=f"Pipeline_{file.name}".replace(".csv", ""),
        description="Flue DLT dans minIO",
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["minio", "pipeline"],
    ) as dag:

        t1 = PythonOperator(
            task_id="import_to_raw",
            python_callable=upload_file,
            op_kwargs={
                "file_path": file,
                "bucket": MINIO_BUCKET_RAW,
            }
        )

        t2 = PythonOperator(
            task_id="transform_to_staging",
            python_callable=transformation_to_staging,
            op_kwargs={
                "file_name": file.name,
                "bucket_from": MINIO_BUCKET_RAW,
                "bucket_to": MINIO_BUCKET_STAGING,
                "pipeline_fqn": f"airflow_local.{dag.dag_id}",
            }
        )

        t3 = PythonOperator(
            task_id="transform_to_curated",
            python_callable=transformation_to_curated,
            op_kwargs={
                "file_name": file.name,
                "bucket_from": MINIO_BUCKET_STAGING,
                "bucket_to": MINIO_BUCKET_CURATED,
                "pipeline_fqn": f"airflow_local.{dag.dag_id}",
            }
        )

        t1 >> t2 >> t3