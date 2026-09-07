from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.minio_connexion import get_minio_client
from scripts.om_lineage import link_buckets


SOURCE_BUCKET = os.getenv("MINIO_BUCKET_CURATED", "curated")
ARCHIVE_BUCKET = os.getenv("MINIO_BUCKET_ARCHIVE", "archive")
RETENTION_DAYS = int(os.getenv("MINIO_ARCHIVE_RETENTION_DAYS", "180"))


def archive_old_objects(**kwargs):
    s3 = get_minio_client()

    try:
        s3.head_bucket(Bucket=ARCHIVE_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=ARCHIVE_BUCKET)

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

    archived_count = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=SOURCE_BUCKET):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                key = obj["Key"]
                # copie vers le bucket archive
                s3.copy_object(
                    Bucket=ARCHIVE_BUCKET,
                    CopySource={"Bucket": SOURCE_BUCKET, "Key": key},
                    Key=key,
                )
                # suppression de la source une fois la copie confirmée
                s3.delete_object(Bucket=SOURCE_BUCKET, Key=key)
                archived_count += 1
                print(f"Archivé : {key}")

    print(f"Archivage terminé : {archived_count} objet(s) déplacé(s) vers {ARCHIVE_BUCKET}")

    if archived_count:
        link_buckets(
            SOURCE_BUCKET,
            ARCHIVE_BUCKET,
            pipeline_fqn=f"airflow_local.{kwargs['dag'].dag_id}",
        )


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minio_archive_180d",
    description="Archive les objets du bucket curated plus vieux que la période de rétention",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance", "minio"],
) as dag:

    archive_task = PythonOperator(
        task_id="archive_old_objects",
        python_callable=archive_old_objects,
    )
