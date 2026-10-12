from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def heroku_safe_path(value: str, default_tmp: str) -> Path:
    """Resolve a path, preferring /tmp on Heroku where the dyno filesystem is ephemeral."""
    path = Path(value)
    if path.is_absolute():
        return path
    resolved = PROJECT_ROOT / path
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        test = resolved.parent / ".write_test"
        test.touch()
        test.unlink()
        return resolved
    except OSError:
        return Path(default_tmp)


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    session_string: str
    download_concurrency: int = 2
    upload_concurrency: int = 1
    stats_ttl_seconds: int = 30
    temp_dir: Path = Path("temp")
    database_path: Path = Path("data/jobs.json")
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        # SESSION_STRING must be set via Heroku dashboard
        if not os.getenv("SESSION_STRING"):
            raise SystemExit("Missing required configuration: SESSION_STRING")

        return cls(
            api_id=20140875,
            api_hash="a06fa97d5a853ec2da79015b11335a17",
            session_string=os.environ["SESSION_STRING"],
            download_concurrency=2,
            upload_concurrency=1,
            stats_ttl_seconds=30,
            temp_dir=heroku_safe_path("temp", "/tmp/telefetch/temp"),
            database_path=heroku_safe_path("data/jobs.json", "/tmp/telefetch/data/jobs.json"),
            log_level="INFO",
        )
