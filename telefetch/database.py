from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

from .jobs import Job


class Database:
    """Small atomic JSON state store for a single Telefetch process."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with self._lock:
            data = await asyncio.to_thread(self._read)
            changed = False
            for record in data["jobs"].values():
                if record["status"] not in {"COMPLETED", "FAILED", "CANCELLED"}:
                    record.update(status="FAILED", error="Service restarted before completion", updated_at=time.time())
                    changed = True
            if changed or not self.path.exists():
                await asyncio.to_thread(self._write, data)

    async def create(self, job: Job) -> bool:
        async with self._lock:
            data = await asyncio.to_thread(self._read)
            if any(record["input_message_id"] == job.input_message_id and record.get("input_chat_id", 0) == job.input_chat_id for record in data["jobs"].values()):
                return False
            now = time.time()
            data["jobs"][job.id] = {
                "id": job.id,
                "input_message_id": job.input_message_id,
                "input_chat_id": job.input_chat_id,
                "source_url": job.source_url,
                "source_chat": job.source_chat,
                "source_message_id": job.source_message_id,
                "status": str(job.status),
                "error": None,
                "output_message_ids": [],
                "created_at": job.created_at,
                "updated_at": now,
            }
            await asyncio.to_thread(self._write, data)
            return True

    async def update(self, job: Job, output_ids: list[int] | None = None) -> None:
        async with self._lock:
            data = await asyncio.to_thread(self._read)
            record = data["jobs"].get(job.id)
            if record is None:
                return
            record.update(status=str(job.status), error=job.error, updated_at=time.time())
            if output_ids is not None:
                record["output_message_ids"] = output_ids
            await asyncio.to_thread(self._write, data)

    async def delete(self, job_id: str) -> None:
        async with self._lock:
            data = await asyncio.to_thread(self._read)
            if data["jobs"].pop(job_id, None) is not None:
                await asyncio.to_thread(self._write, data)

    async def get_dump_chat_id(self) -> int | None:
        async with self._lock:
            data = await asyncio.to_thread(self._read)
            value = data.get("settings", {}).get("dump_chat_id")
            return int(value) if value is not None else None

    async def set_dump_chat_id(self, chat_id: int | None) -> None:
        async with self._lock:
            data = await asyncio.to_thread(self._read)
            data.setdefault("settings", {})["dump_chat_id"] = chat_id
            await asyncio.to_thread(self._write, data)

    def _read(self) -> dict:
        if not self.path.exists():
            return {"version": 1, "jobs": {}, "settings": {}}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Cannot read JSON state file {self.path}") from exc
        if not isinstance(value, dict) or not isinstance(value.get("jobs"), dict):
            raise RuntimeError(f"Invalid JSON state structure in {self.path}")
        value.setdefault("settings", {})
        return value

    def _write(self, data: dict) -> None:
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, self.path)
