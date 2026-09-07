import pandas as pd
from pathlib import Path
from scripts.minio_connexion import get_minio_client, DATA_PREFIX
from scripts.om_lineage import link_buckets


def transformation_to_staging(file_name, bucket_from, bucket_to, pipeline_fqn=None):

    s3 = get_minio_client()
    key = f"{DATA_PREFIX}/{file_name}"
    staging_dir = Path('data/staging')
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_path = staging_dir / file_name
    s3.download_file(bucket_from, key, str(staging_path))

    print("--- Transformation to staging ---")

    df = pd.read_csv(str(staging_path))

    print(f"Original columns: {df.columns.tolist()}")
    df.columns = df.columns.str.lower()
    print(f"Transformed columns: {df.columns.tolist()}")


    df.to_csv(str(staging_path), index=False)


    s3.upload_file(str(staging_path), bucket_to, key)

    link_buckets(bucket_from, bucket_to, pipeline_fqn=pipeline_fqn)
