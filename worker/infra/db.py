from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


@dataclass
class ReportJob:
    id: str
    user_id: str | None
    status: str
    topic: str | None
    input_payload_json: Any | None
    output_format: str | None
    output_s3_key: str | None


class ReportJobRepository:
    """Small repository layer for report_jobs table operations."""

    def __init__(self, database_url: str) -> None:
        self.engine: Engine = create_engine(database_url, pool_pre_ping=True, future=True)
        self.session_factory = sessionmaker(bind=self.engine, autocommit=False, autoflush=False, future=True)

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_job(self, job_id: str) -> ReportJob | None:
        query = text(
            """
            SELECT id, user_id, status, topic, input_payload_json, output_format, output_s3_key
            FROM report_jobs
            WHERE id = :job_id
            """
        )
        with self.session_scope() as session:
            row = session.execute(query, {"job_id": job_id}).mappings().first()
            if not row:
                return None
            return ReportJob(
                id=str(row["id"]),
                user_id=str(row["user_id"]) if row["user_id"] is not None else None,
                status=str(row["status"]),
                topic=row["topic"],
                input_payload_json=row["input_payload_json"],
                output_format=row["output_format"],
                output_s3_key=row["output_s3_key"],
            )

    def mark_processing(self, job_id: str, stage: str, progress_percent: int) -> None:
        now = datetime.now(timezone.utc)
        query = text(
            """
            UPDATE report_jobs
            SET status = 'processing',
                progress_percent = :progress_percent,
                current_stage = :stage,
                started_at = COALESCE(started_at, :now),
                error_code = NULL,
                error_message = NULL,
                failed_stage = NULL
            WHERE id = :job_id
              AND status IN ('queued', 'pending', 'retry', 'processing')
            """
        )
        with self.session_scope() as session:
            session.execute(
                query,
                {
                    "job_id": job_id,
                    "progress_percent": progress_percent,
                    "stage": stage,
                    "now": now,
                },
            )

    def update_progress(self, job_id: str, stage: str, progress_percent: int) -> None:
        query = text(
            """
            UPDATE report_jobs
            SET current_stage = :stage,
                progress_percent = :progress_percent
            WHERE id = :job_id
            """
        )
        with self.session_scope() as session:
            session.execute(
                query,
                {
                    "job_id": job_id,
                    "stage": stage,
                    "progress_percent": progress_percent,
                },
            )

    def mark_completed(self, job_id: str, output_s3_key: str) -> None:
        now = datetime.now(timezone.utc)
        query = text(
            """
            UPDATE report_jobs
            SET status = 'completed',
                progress_percent = 100,
                current_stage = 'completed',
                output_s3_key = :output_s3_key,
                completed_at = :now,
                error_code = NULL,
                error_message = NULL,
                failed_stage = NULL
            WHERE id = :job_id
            """
        )
        with self.session_scope() as session:
            session.execute(query, {"job_id": job_id, "output_s3_key": output_s3_key, "now": now})

    def mark_failed(self, job_id: str, error_code: str, error_message: str, failed_stage: str) -> None:
        query = text(
            """
            UPDATE report_jobs
            SET status = 'failed',
                current_stage = :failed_stage,
                error_code = :error_code,
                error_message = :error_message,
                failed_stage = :failed_stage
            WHERE id = :job_id
            """
        )
        with self.session_scope() as session:
            session.execute(
                query,
                {
                    "job_id": job_id,
                    "error_code": error_code,
                    "error_message": error_message[:2000],
                    "failed_stage": failed_stage,
                },
            )
