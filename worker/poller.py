from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

from worker.infra.sqs import SQSClient, SQSMessage
from worker.processor import JobProcessor

logger = logging.getLogger(__name__)


class WorkerPoller:
    """SQS poller that processes jobs with bounded local concurrency."""

    def __init__(
        self,
        sqs_client: SQSClient,
        processor: JobProcessor,
        max_workers: int,
        long_poll_seconds: int,
        max_messages: int,
        idle_sleep_seconds: float,
    ) -> None:
        self.sqs_client = sqs_client
        self.processor = processor
        self.max_workers = max(1, max_workers)
        self.long_poll_seconds = long_poll_seconds
        self.max_messages = max(1, min(max_messages, 10))
        self.idle_sleep_seconds = max(0.1, idle_sleep_seconds)

        self._executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="job-worker")
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._futures: dict[Future, SQSMessage] = {}
        self._lock = threading.Lock()
        self._running = False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="sqs-poller", daemon=True)
        self._thread.start()
        self._running = True
        logger.info("poller_started max_workers=%s", self.max_workers)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self._executor.shutdown(wait=True, cancel_futures=False)
        self._running = False
        logger.info("poller_stopped")

    def is_running(self) -> bool:
        return self._running and not self._stop_event.is_set()

    def inflight_count(self) -> int:
        with self._lock:
            return len(self._futures)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._collect_finished()
            capacity = self.max_workers - self.inflight_count()
            if capacity <= 0:
                time.sleep(self.idle_sleep_seconds)
                continue

            try:
                messages = self.sqs_client.receive_messages(
                    max_messages=min(self.max_messages, capacity),
                    wait_time_seconds=self.long_poll_seconds,
                )
            except Exception:
                logger.exception("sqs_receive_failed")
                time.sleep(self.idle_sleep_seconds)
                continue

            if not messages:
                continue

            for message in messages:
                future = self._executor.submit(self.processor.process, message.body)
                with self._lock:
                    self._futures[future] = message

        self._collect_finished()

    def _collect_finished(self) -> None:
        finished: list[Future] = []
        with self._lock:
            for future in self._futures:
                if future.done():
                    finished.append(future)

        for future in finished:
            with self._lock:
                message = self._futures.pop(future, None)

            if message is None:
                continue

            try:
                result = future.result()
            except Exception:
                logger.exception("worker_future_failed message_id=%s", message.message_id)
                continue

            if result.success:
                try:
                    self.sqs_client.delete_message(message.receipt_handle)
                    logger.info("sqs_message_deleted message_id=%s job_id=%s", message.message_id, result.job_id)
                except Exception:
                    logger.exception("sqs_delete_failed message_id=%s", message.message_id)
            else:
                logger.warning(
                    "message_processing_failed message_id=%s job_id=%s error=%s",
                    message.message_id,
                    result.job_id,
                    result.error,
                )
