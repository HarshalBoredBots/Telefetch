from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "QUEUED"
    RESOLVING = "RESOLVING"
    DOWNLOADING = "DOWNLOADING"
    SPLITTING = "SPLITTING"
    UPLOADING = "UPLOADING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class Job:
    input_message_id: int
    source_url: str
    input_chat_id: int = 0
    source_chat: str | int | None = None
    source_message_id: int | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:10])
    status: JobStatus = JobStatus.QUEUED
    current: int = 0
    total: int = 0
    speed: float = 0
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    _sample_time: float = field(default_factory=time.monotonic, repr=False)
    _sample_bytes: int = field(default=0, repr=False)

    def progress(self, current: int, total: int) -> None:
        now = time.monotonic()
        elapsed = now - self._sample_time
        if elapsed >= 1:
            instant = max(0, current - self._sample_bytes) / elapsed
            self.speed = instant if not self.speed else self.speed * 0.65 + instant * 0.35
            self._sample_time, self._sample_bytes = now, current
        self.current, self.total = current, total
        if self.cancel_event.is_set():
            raise asyncio.CancelledError

    @property
    def percent(self) -> float:
        return self.current * 100 / self.total if self.total else 0

    @property
    def eta(self) -> float | None:
        return (self.total - self.current) / self.speed if self.speed and self.total else None
