from __future__ import annotations

import json
import logging
import time
from typing import Any

import boto3


LOGGER = logging.getLogger(__name__)


class KinesisPublisher:
    def __init__(
        self,
        stream_name: str,
        region_name: str,
        publish_retries: int = 3,
        publish_backoff_seconds: float = 1.0,
    ) -> None:
        self.stream_name = stream_name
        self.publish_retries = publish_retries
        self.publish_backoff_seconds = publish_backoff_seconds
        self.client = boto3.client("kinesis", region_name=region_name)

    def publish(self, record: dict[str, Any], partition_key: str) -> dict[str, Any]:
        payload = json.dumps(record).encode("utf-8")
        attempt = 1
        while True:
            try:
                response = self.client.put_record(
                    StreamName=self.stream_name,
                    Data=payload,
                    PartitionKey=partition_key,
                )
                LOGGER.debug(
                    "Published record to Kinesis stream=%s shard=%s sequence=%s",
                    self.stream_name,
                    response.get("ShardId"),
                    response.get("SequenceNumber"),
                )
                return response
            except Exception:
                if attempt >= self.publish_retries:
                    LOGGER.exception(
                        "Failed to publish record to Kinesis stream=%s after %s attempts",
                        self.stream_name,
                        attempt,
                    )
                    raise

                LOGGER.warning(
                    "Retrying Kinesis publish stream=%s attempt=%s/%s",
                    self.stream_name,
                    attempt + 1,
                    self.publish_retries,
                )
                time.sleep(self.publish_backoff_seconds * attempt)
                attempt += 1
