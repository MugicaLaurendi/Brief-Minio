import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv

load_dotenv()

# Sous-dossier commun aux buckets raw/staging/curated : OpenMetadata ne peut
# extraire le schema d'un fichier via son manifest que si celui-ci vit dans un
# "dossier" (dataPath) plutot qu'a la racine du bucket.
DATA_PREFIX = "lines"

def get_minio_client():
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    secure = os.getenv('MINIO_SECURE', 'false').strip().lower() in ('1', 'true', 'yes')
    region = os.getenv('MINIO_REGION')

    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name=region,
        use_ssl=secure,
    )