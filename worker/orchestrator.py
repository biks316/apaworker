from __future__ import annotations

from worker.config import Settings
from worker.infra.db import ReportJobRepository
from worker.infra.s3 import S3Client
from worker.infra.sqs import SQSClient
from worker.poller import WorkerPoller
from worker.processor import JobProcessor
from worker.services.export_service import ExportService


class WorkerOrchestrator:
    """Wires and controls worker dependencies."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

        repo = ReportJobRepository(settings.database_url)
        s3_client = S3Client(bucket=settings.s3_bucket, region_name=settings.aws_region)
        sqs_client = SQSClient(
            queue_url=settings.sqs_queue_url,
            region_name=settings.aws_region,
            visibility_timeout_seconds=settings.sqs_visibility_timeout_seconds,
        )
        export_service = ExportService()
        processor = JobProcessor(
            repo=repo,
            s3_client=s3_client,
            export_service=export_service,
            s3_output_prefix=settings.s3_output_prefix,
        )

        self.poller = WorkerPoller(
            sqs_client=sqs_client,
            processor=processor,
            max_workers=settings.max_workers,
            long_poll_seconds=settings.sqs_long_poll_seconds,
            max_messages=settings.sqs_max_messages,
            idle_sleep_seconds=settings.poller_idle_sleep_seconds,
        )

    def start(self) -> None:
        self.poller.start()

    def stop(self) -> None:
        self.poller.stop()

    def is_ready(self) -> bool:
        return self.poller.is_running()

    def inflight_count(self) -> int:
        return self.poller.inflight_count()
