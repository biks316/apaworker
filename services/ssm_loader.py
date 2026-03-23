from __future__ import annotations

import os
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

OPENAI_API_KEY_PARAMETER = "/apaworker/dev/openai/api_key"


class SSMParameterError(RuntimeError):
    """Raised when the API key cannot be loaded from SSM."""


@lru_cache(maxsize=1)
def load_openai_api_key() -> str:
    """Load and cache the OpenAI API key from AWS SSM Parameter Store."""
    region = os.getenv("AWS_REGION", "us-east-1")
    client = boto3.client("ssm", region_name=region)

    try:
        response = client.get_parameter(Name=OPENAI_API_KEY_PARAMETER, WithDecryption=True)
    except (NoCredentialsError, PartialCredentialsError) as exc:
        raise SSMParameterError("AWS credentials are missing or incomplete for SSM access.") from exc
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "ParameterNotFound":
            raise SSMParameterError(
                f"SSM parameter not found: {OPENAI_API_KEY_PARAMETER}"
            ) from exc
        raise SSMParameterError("Unable to access AWS SSM Parameter Store.") from exc

    value = response.get("Parameter", {}).get("Value", "").strip()
    if not value:
        raise SSMParameterError(
            f"SSM parameter {OPENAI_API_KEY_PARAMETER} is empty or invalid."
        )

    return value
