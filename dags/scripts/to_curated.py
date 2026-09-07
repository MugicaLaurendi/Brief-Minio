import pandas as pd
from pathlib import Path
from scripts.minio_connexion import get_minio_client, DATA_PREFIX
from scripts.om_lineage import link_buckets


def transformation_to_curated(file_name, bucket_from, bucket_to, pipeline_fqn=None):

    s3 = get_minio_client()
    key = f"{DATA_PREFIX}/{file_name}"
    curated_dir = Path('data/curated')
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated_path = curated_dir / file_name
    s3.download_file(bucket_from, key, str(curated_path))

    print("--- Transformation to curated ---")

    df = pd.read_csv(str(curated_path))

    # faire les transformations


    df.to_csv(str(curated_path), index=False)


    s3.upload_file(str(curated_path), bucket_to, key)

    link_buckets(bucket_from, bucket_to, pipeline_fqn=pipeline_fqn)
