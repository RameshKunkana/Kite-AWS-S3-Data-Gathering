from __future__ import annotations

import json
import logging
import queue
import threading
import time
from typing import Any

import boto3


LOGGER = logging.getLogger(__name__)
_SENTINEL = object()
_MAX_PUT_RECORDS_BATCH = 500


class KinesisPublisher:
    def __init__(
        self,
        stream_name: str,
        region_name: str,
        publish_retries: int = 3,
        publish_backoff_seconds: float = 1.0,
        batch_size: int = 100,
        flush_interval_ms: int = 50,
        max_queue_size: int = 10000,
    ) -> None:
        self.stream_name = stream_name
        self.publish_retries = publish_retries
        self.publish_backoff_seconds = publish_backoff_seconds
        self.batch_size = max(1, min(batch_size, _MAX_PUT_RECORDS_BATCH))
        self.flush_interval_seconds = max(flush_interval_ms, 1) / 1000
        self.client = boto3.client("kinesis", region_name=region_name)
        self._queue: queue.Queue[dict[str, bytes | str] | object] = queue.Queue(maxsize=max_queue_size)
        self._closed = False
        self._worker = threading.Thread(target=self._run, name="kinesis-batch-publisher", daemon=True)
        self._worker.start()

    def publish(self, record: dict[str, Any], partition_key: str) -> None:
        if self._closed:
            raise RuntimeError("Cannot publish to a closed KinesisPublisher")

        payload = json.dumps(record, separators=(",", ":")).encode("utf-8")
        item: dict[str, bytes | str] = {"Data": payload, "PartitionKey": partition_key}
        self._queue.put(item)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._queue.put(_SENTINEL)
        self._worker.join()

    def _run(self) -> None:
        batch: list[dict[str, bytes | str]] = []
        next_flush_at = time.monotonic() + self.flush_interval_seconds

        while True:
            timeout = max(0.0, next_flush_at - time.monotonic())
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                if batch:
                    self._publish_batch(batch)
                    batch.clear()
                next_flush_at = time.monotonic() + self.flush_interval_seconds
                continue

            if item is _SENTINEL:
                if batch:
                    self._publish_batch(batch)
                break

            batch.append(item)
            if len(batch) >= self.batch_size:
                self._publish_batch(batch)
                batch.clear()
                next_flush_at = time.monotonic() + self.flush_interval_seconds

    def _publish_batch(self, batch: list[dict[str, bytes | str]]) -> None:
        pending = list(batch)
        attempt = 1

        while pending:
            try:
                response = self.client.put_records(StreamName=self.stream_name, Records=pending)
            except Exception:
                if attempt >= self.publish_retries:
                    LOGGER.exception(
                        "Failed to publish %s batched records to Kinesis stream=%s after %s attempts",
                        len(pending),
                        self.stream_name,
                        attempt,
                    )
                    raise

                LOGGER.warning(
                    "Retrying Kinesis batch publish stream=%s attempt=%s/%s batch_size=%s",
                    self.stream_name,
                    attempt + 1,
                    self.publish_retries,
                    len(pending),
                )
                time.sleep(self.publish_backoff_seconds * attempt)
                attempt += 1
                continue

            failed_records = [
                record
                for record, result in zip(pending, response.get("Records", []), strict=False)
                if result.get("ErrorCode")
            ]
            failed_count = response.get("FailedRecordCount", 0)
            success_count = len(pending) - failed_count
            LOGGER.debug(
                "Published batch to Kinesis stream=%s success=%s failed=%s",
                self.stream_name,
                success_count,
                failed_count,
            )

            if not failed_records:
                return

            if attempt >= self.publish_retries:
                LOGGER.error(
                    "Failed to publish %s Kinesis records to stream=%s after %s attempts",
                    len(failed_records),
                    self.stream_name,
                    attempt,
                )
                raise RuntimeError(
                    f"Failed to publish {len(failed_records)} Kinesis records after retries"
                )

            LOGGER.warning(
                "Retrying %s failed Kinesis records stream=%s attempt=%s/%s",
                len(failed_records),
                self.stream_name,
                attempt + 1,
                self.publish_retries,
            )
            time.sleep(self.publish_backoff_seconds * attempt)
            pending = failed_records
            attempt += 1
