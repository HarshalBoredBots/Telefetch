from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlsplit


class InvalidLink(ValueError):
    pass


@dataclass(frozen=True)
class LinkReference:
    original_url: str
    chat: str | int
    message_id: int
    topic_id: int | None = None
    is_private: bool = False


URL_RE = re.compile(r"https?://(?:t|telegram)\.me/[^\s<>]+", re.IGNORECASE)


def extract_telegram_url(text: str) -> str:
    match = URL_RE.search(text)
    if not match:
        raise InvalidLink("The replied message does not contain a Telegram post link")
    return match.group(0).rstrip(".,;:!?)]}")


def parse_telegram_link(url: str) -> LinkReference:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"t.me", "telegram.me", "www.t.me", "www.telegram.me"}:
        raise InvalidLink("Only t.me or telegram.me post links are supported")
    parts = [p for p in parsed.path.split("/") if p]
    if parts and parts[0] == "s":
        parts.pop(0)
    if len(parts) < 2:
        raise InvalidLink("The link must point to a specific Telegram message")
    if parts[0] in {"joinchat", "+", "addlist", "share", "iv"}:
        raise InvalidLink("Invite and utility links are not message links")
    private = parts[0] == "c"
    try:
        if private:
            if len(parts) not in {3, 4}:
                raise InvalidLink("Malformed private message link")
            internal = parts[1]
            if not internal.isdigit():
                raise InvalidLink("Malformed private chat identifier")
            numeric = [int(p) for p in parts[2:]]
            chat: str | int = int(f"-100{internal}")
        else:
            if len(parts) not in {2, 3}:
                raise InvalidLink("Malformed public message link")
            if not re.fullmatch(r"[A-Za-z0-9_]{5,}", parts[0]):
                raise InvalidLink("Malformed public chat username")
            numeric = [int(p) for p in parts[1:]]
            chat = parts[0]
    except ValueError as exc:
        raise InvalidLink("Message and topic identifiers must be numbers") from exc
    if any(value <= 0 for value in numeric):
        raise InvalidLink("Message and topic identifiers must be positive")
    return LinkReference(url, chat, numeric[-1], numeric[0] if len(numeric) == 2 else None, private)
