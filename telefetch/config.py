from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def project_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def heroku_safe_path(value: str, default_tmp: str) -> Path:
    """Resolve a path, preferring /tmp on Heroku where the dyno filesystem is ephemeral."""
    path = Path(value)
    if path.is_absolute():
        return path
    # On Heroku, relative paths under the slug are read-only after deploy.
    # Fall back to /tmp which is writable but ephemeral between dyno restarts.
    resolved = PROJECT_ROOT / path
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        # Quick write-test so we don't silently use a non-writable location
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
        # Only load .env when it actually exists (local dev). On Heroku all
        # values come from config vars set via `heroku config:set`.
        env_file = PROJECT_ROOT / ".env"
        if env_file.exists():
            load_dotenv(env_file)
        missing = [key for key in ("API_ID", "API_HASH", "SESSION_STRING") if not os.getenv(key)]
        if missing:
            raise SystemExit(f"Missing required configuration: {', '.join(missing)}")
        try:
            api_id = int(os.environ["API_ID"])
        except ValueError as exc:
            raise SystemExit("API_ID must be an integer") from exc
        return cls(
            api_id=api_id,
            api_hash=os.environ["API_HASH"],
            session_string=os.environ["SESSION_STRING"],
            download_concurrency=max(1, int(os.getenv("DOWNLOAD_CONCURRENCY", "2"))),
            upload_concurrency=max(1, int(os.getenv("UPLOAD_CONCURRENCY", "1"))),
            stats_ttl_seconds=max(5, int(os.getenv("STATS_TTL_SECONDS", "30"))),
            temp_dir=heroku_safe_path(os.getenv("TEMP_DIR", "temp"), "/tmp/telefetch/temp"),
            database_path=heroku_safe_path(os.getenv("DATABASE_PATH", "data/jobs.json"), "/tmp/telefetch/data/jobs.json"),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )
