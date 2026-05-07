import hashlib

import boto3
import psycopg

from rico import config


def compute_fingerprint(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_postgres_conn() -> psycopg.Connection:
    return psycopg.connect(config.POSTGRES_DSN)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
