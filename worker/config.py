from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    app_env: str = Field(default="dev", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")
    sqs_queue_url: str = Field(alias="SQS_QUEUE_URL")
    sqs_long_poll_seconds: int = Field(default=20, alias="SQS_LONG_POLL_SECONDS")
    sqs_visibility_timeout_seconds: int = Field(default=300, alias="SQS_VISIBILITY_TIMEOUT_SECONDS")
    sqs_max_messages: int = Field(default=5, alias="SQS_MAX_MESSAGES")

    s3_bucket: str = Field(alias="S3_BUCKET")
    s3_output_prefix: str = Field(default="reports", alias="S3_OUTPUT_PREFIX")

    database_url: str = Field(alias="DATABASE_URL")

    max_workers: int = Field(default=2, alias="WORKER_MAX_WORKERS")
    poller_idle_sleep_seconds: float = Field(default=1.0, alias="WORKER_IDLE_SLEEP_SECONDS")

    health_host: str = Field(default="0.0.0.0", alias="HEALTH_HOST")
    health_port: int = Field(default=8001, alias="HEALTH_PORT")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
