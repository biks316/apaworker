from __future__ import annotations

from pathlib import Path

import boto3
from botocore.client import BaseClient


class S3Client:
    def __init__(self, bucket: str, region_name: str) -> None:
        self.bucket = bucket
        self.client: BaseClient = boto3.client("s3", region_name=region_name)

    def upload_file(self, local_path: Path, key: str, content_type: str = "text/plain") -> str:
        self.client.upload_file(
            Filename=str(local_path),
            Bucket=self.bucket,
            Key=key,
            ExtraArgs={"ContentType": content_type},
        )
        return key
