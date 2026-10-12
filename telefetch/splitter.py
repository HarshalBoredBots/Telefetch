from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Callable


def split_file(path: Path, part_size: int, progress: Callable[[int, int], None] | None = None) -> tuple[list[Path], Path]:
    total = path.stat().st_size
    digest = hashlib.sha256()
    parts: list[Path] = []
    done = 0
    with path.open("rb") as source:
        index = 1
        while True:
            part = path.with_name(f"{path.name}.part{index:03d}")
            written = 0
            with part.open("wb") as target:
                while written < part_size:
                    chunk = source.read(min(8 * 1024 * 1024, part_size - written))
                    if not chunk:
                        break
                    target.write(chunk)
                    digest.update(chunk)
                    written += len(chunk)
                    done += len(chunk)
                    if progress:
                        progress(done, total)
            if not written:
                part.unlink(missing_ok=True)
                break
            parts.append(part)
            index += 1
    manifest = path.with_name(f"{path.name}.parts.json")
    manifest.write_text(json.dumps({"filename": path.name, "size": total, "sha256": digest.hexdigest(), "parts": [p.name for p in parts]}, indent=2), encoding="utf-8")
    return parts, manifest

