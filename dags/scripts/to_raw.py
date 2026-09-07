
import os
from boto3.s3.transfer import TransferConfig
from scripts.minio_connexion import get_minio_client, DATA_PREFIX

# Taille de chunk pour l'upload multipart (5 Mo = minimum accepté par S3/MinIO pour une part)
UPLOAD_CHUNK_SIZE = int(os.getenv("MINIO_UPLOAD_CHUNK_SIZE", 8 * 1024 * 1024))


def upload_file(file_path, bucket):

    s3 = get_minio_client()

    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    local_path = file_path
    filename = os.path.relpath(local_path, os.path.dirname(file_path))
    key = os.path.join(DATA_PREFIX, filename).replace("\\", "/")  # S3 uses forward slashes

    # multipart_threshold = multipart_chunksize pour forcer un upload par chunk
    # dès que le fichier dépasse une part, plutôt que d'attendre un gros seuil.
    transfer_config = TransferConfig(
        multipart_threshold=UPLOAD_CHUNK_SIZE,
        multipart_chunksize=UPLOAD_CHUNK_SIZE,
    )

    s3.upload_file(local_path, bucket, key, Config=transfer_config)
    print(f"Uploaded: {key} (chunk size: {UPLOAD_CHUNK_SIZE} bytes)")