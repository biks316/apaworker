from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
import tempfile

from worker.infra.db import ReportJobRepository
from worker.infra.s3 import S3Client
from worker.services.export_service import ExportService

logger = logging.getLogger(__name__)


@dataclass
class ParsedMessage:
    job_id: str
    event_type: str


@dataclass
class ProcessingResult:
    success: bool
    job_id: str | None = None
    error: str | None = None


class JobProcessor:
    def __init__(
        self,
        repo: ReportJobRepository,
        s3_client: S3Client,
        export_service: ExportService,
        s3_output_prefix: str,
    ) -> None:
        self.repo = repo
        self.s3_client = s3_client
        self.export_service = export_service
        self.s3_output_prefix = s3_output_prefix.strip("/")

    def parse_message(self, body: str) -> ParsedMessage:
        data = json.loads(body)
        job_id = data.get("job_id")
        event_type = data.get("event_type")
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("Invalid SQS message: 'job_id' is required")
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("Invalid SQS message: 'event_type' is required")
        return ParsedMessage(job_id=job_id, event_type=event_type)

    def process(self, body: str) -> ProcessingResult:
        failed_stage = "message_parse"
        parsed: ParsedMessage | None = None
        try:
            parsed = self.parse_message(body)
            failed_stage = "load_job"
            job = self.repo.get_job(parsed.job_id)
            if job is None:
                raise ValueError(f"Job not found: {parsed.job_id}")

            if job.status == "completed" and job.output_s3_key:
                logger.info("job_already_completed job_id=%s", parsed.job_id)
                return ProcessingResult(success=True, job_id=parsed.job_id)

            failed_stage = "mark_processing"
            self.repo.mark_processing(parsed.job_id, stage="processing", progress_percent=10)

            failed_stage = "generate_report"
            self.repo.update_progress(parsed.job_id, stage="generate_report", progress_percent=40)
            with tempfile.TemporaryDirectory(prefix=f"report-{parsed.job_id}-") as tmp_dir:
                report_path = self.export_service.generate_placeholder_report(
                    output_dir=Path(tmp_dir),
                    job_id=parsed.job_id,
                    topic=job.topic,
                    payload=job.input_payload_json,
                )

                failed_stage = "upload_report"
                self.repo.update_progress(parsed.job_id, stage="upload_report", progress_percent=75)
                key_prefix = f"{self.s3_output_prefix}/{parsed.job_id}" if self.s3_output_prefix else parsed.job_id
                output_s3_key = self.s3_client.upload_file(
                    local_path=report_path,
                    key=f"{key_prefix}/report.txt",
                    content_type="text/plain",
                )

            failed_stage = "mark_completed"
            self.repo.mark_completed(parsed.job_id, output_s3_key=output_s3_key)
            logger.info("job_completed job_id=%s s3_key=%s", parsed.job_id, output_s3_key)
            return ProcessingResult(success=True, job_id=parsed.job_id)

        except Exception as exc:
            error_message = str(exc)
            logger.exception(
                "job_processing_failed stage=%s job_id=%s error=%s",
                failed_stage,
                parsed.job_id if parsed else None,
                error_message,
            )
            if parsed is not None:
                try:
                    self.repo.mark_failed(
                        parsed.job_id,
                        error_code="PROCESSING_ERROR",
                        error_message=error_message,
                        failed_stage=failed_stage,
                    )
                except Exception:
                    logger.exception("failed_to_mark_job_failed job_id=%s", parsed.job_id)
            return ProcessingResult(
                success=False,
                job_id=parsed.job_id if parsed else None,
                error=error_message,
            )
