from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

OPENAI_API_KEY_PARAMETER = "/apaworker/dev/openai/api_key"
LOCAL_ENV_FILES = (".env", ".github/workflows/.env")


class SSMParameterError(RuntimeError):
    """Raised when the API key cannot be loaded from SSM."""


def _clean_value(raw: str) -> str:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1].strip()
    return value


def _load_api_key_from_env() -> str:
    for key in ("OPENAI_API_KEY", "CHATGPTKEY", "chatgptkey"):
        value = os.getenv(key, "").strip()
        if value:
            return value
    return ""


def _load_api_key_from_local_files() -> str:
    for file_path in LOCAL_ENV_FILES:
        path = Path(file_path)
        if not path.exists():
            continue

        for line in path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            if "=" in raw:
                key, value = raw.split("=", 1)
            elif ":" in raw:
                key, value = raw.split(":", 1)
            else:
                continue

            if key.strip() in {"OPENAI_API_KEY", "CHATGPTKEY", "chatgptkey"}:
                parsed = _clean_value(value)
                if parsed:
                    return parsed
    return ""


@lru_cache(maxsize=1)
def load_openai_api_key() -> str:
    """Load and cache the OpenAI API key from AWS SSM Parameter Store."""
    local_key = _load_api_key_from_env() or _load_api_key_from_local_files()
    if local_key:
        return local_key

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
