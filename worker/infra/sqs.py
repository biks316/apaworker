from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.client import BaseClient


@dataclass
class SQSMessage:
    message_id: str
    receipt_handle: str
    body: str


class SQSClient:
    def __init__(self, queue_url: str, region_name: str, visibility_timeout_seconds: int) -> None:
        self.queue_url = queue_url
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self.client: BaseClient = boto3.client("sqs", region_name=region_name)

    def receive_messages(self, max_messages: int, wait_time_seconds: int) -> list[SQSMessage]:
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max(1, min(max_messages, 10)),
            WaitTimeSeconds=max(0, min(wait_time_seconds, 20)),
            VisibilityTimeout=self.visibility_timeout_seconds,
        )
        messages = response.get("Messages", [])
        return [
            SQSMessage(
                message_id=msg["MessageId"],
                receipt_handle=msg["ReceiptHandle"],
                body=msg["Body"],
            )
            for msg in messages
        ]

    def delete_message(self, receipt_handle: str) -> None:
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
