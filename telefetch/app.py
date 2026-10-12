from __future__ import annotations

import asyncio
import logging
import shutil
import time
from contextlib import suppress
from pathlib import Path

from pyrogram import Client
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import InputMediaAudio, InputMediaDocument, InputMediaPhoto, InputMediaVideo, Message

from .config import Settings
from .database import Database
from .formatting import human_bytes, human_duration
from .jobs import Job, JobStatus
from .links import InvalidLink, extract_telegram_url, parse_telegram_link
from .splitter import split_file

log = logging.getLogger("telefetch")
TERMINAL = {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}


class TelefetchApp:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = Client("telefetch", api_id=settings.api_id, api_hash=settings.api_hash, session_string=settings.session_string, in_memory=True)
        self.database = Database(settings.database_path)
        self.queue: asyncio.Queue[Job] = asyncio.Queue()
        self.jobs: dict[str, Job] = {}
        self.download_slots = asyncio.Semaphore(settings.download_concurrency)
        self.upload_slots = asyncio.Semaphore(settings.upload_concurrency)
        self.tasks: set[asyncio.Task] = set()
        self.handled_command_ids: set[tuple[int, int]] = set()
        self.me_id = 0
        self.max_part_size = 1_950_000_000
        self.started_at = time.monotonic()
        self.dump_chat_id: int | None = None

    async def run(self) -> None:
        logging.basicConfig(level=self.settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        log.info("Starting Telefetch")
        self.settings.temp_dir.mkdir(parents=True, exist_ok=True)
        await self._cleanup_abandoned()
        await self.database.initialize()
        self.dump_chat_id = await self.database.get_dump_chat_id()
        self._register_handlers()
        log.info("Connecting to Telegram")
        try:
            await asyncio.wait_for(self.client.start(), timeout=45)
        except TimeoutError as exc:
            raise RuntimeError("Telegram connection timed out before authentication completed") from exc
        me = await self.client.get_me()
        self.me_id = me.id
        self.max_part_size = 3_950_000_000 if bool(getattr(me, "is_premium", False)) else 1_950_000_000
        log.info("Authenticated as account %s; upload part limit %s", self.me_id, human_bytes(self.max_part_size))
        workers = [asyncio.create_task(self._worker(i)) for i in range(self.settings.download_concurrency)]
        poller = asyncio.create_task(self._poll_saved_messages())
        try:
            await asyncio.Event().wait()
        finally:
            for task in [*workers, poller, *self.tasks]:
                task.cancel()
            await asyncio.gather(*workers, poller, *self.tasks, return_exceptions=True)
            await self.client.stop()

    def _register_handlers(self) -> None:
        @self.client.on_message()
        async def command_handler(_: Client, message: Message) -> None:
            # Do not depend on filters.me/filters.outgoing: Saved Messages updates
            # differ between Telegram/Pyrofork versions. An outgoing update is
            # necessarily authored by this authenticated session.
            # Saved Messages is the authenticated user's private self-chat. Some
            # Pyrofork update shapes omit both `outgoing` and `from_user` there,
            # so the chat identity is the reliable ownership signal.
            authored_by_me = (
                message.chat.id == self.me_id
                or bool(message.outgoing)
                or bool(message.from_user and message.from_user.id == self.me_id)
            )
            if not authored_by_me:
                return
            key = (message.chat.id, message.id)
            if key in self.handled_command_ids:
                return
            self.handled_command_ids.add(key)
            await self._handle_command(message)

    async def _handle_command(self, message: Message) -> None:
        text = (message.text or message.caption or "").strip()
        command = text.split(maxsplit=1)[0].lower().split("@", 1)[0] if text else ""
        if command == "/get":
            log.info("Received /get in chat %s, message %s", message.chat.id, message.id)
            await self._accept(message)
        elif command == "/get_link":
            log.info("Received /get_link in chat %s, message %s", message.chat.id, message.id)
            await self._accept_link(message)
        elif command == "/help" and message.chat.id == self.me_id:
            await self._help(message)
        elif command == "/start" and message.chat.id == self.me_id:
            await self._start(request=message)
        elif command == "/stats" and message.chat.id == self.me_id:
            await self._stats(message)
        elif command == "/cancel" and message.chat.id == self.me_id:
            await self._cancel(message)
        elif command == "/dump" and message.chat.id == self.me_id:
            await self._dump(message)

    async def _poll_saved_messages(self) -> None:
        """Fallback for clients that omit other-session outgoing updates."""
        log.info("Saved Messages command polling started")
        while True:
            try:
                async for message in self.client.get_chat_history("me", limit=20):
                    text = (message.text or message.caption or "").strip()
                    command = text.lower().split(maxsplit=1)[0].split("@", 1)[0] if text else ""
                    key = (message.chat.id, message.id)
                    if (
                        key not in self.handled_command_ids
                        and command in {"/get", "/get_link", "/help", "/start", "/stats", "/cancel", "/dump"}
                    ):
                        self.handled_command_ids.add(key)
                        await self._handle_command(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Could not poll Saved Messages")
            await asyncio.sleep(2)

    async def _help(self, request: Message) -> None:
        text = (
            "**Telefetch Help**\n\n"
            "Download Telegram photos, videos, documents, PDFs, music, audio, "
            "voice messages, stickers, GIFs, video notes, and albums.\n\n"
            "**Service status**\n"
            "Show how long Telefetch has been running. Uptime is shown in seconds, "
            "minutes, hours, or days.\n"
            "`/start`\n\n"
            "**Download by reply**\n"
            "Reply to any Telegram media message with:\n"
            "`/get`\n\n"
            "**Download by link**\n"
            "Use a public or private Telegram message link:\n"
            "`/get_link https://t.me/channel/123`\n\n"
            "**Download status**\n"
            "Each page shows up to 10 active jobs. Page 1 opens by default:\n"
            "`/stats`\n\n"
            "Open a specific page:\n"
            "`/stats 2`\n\n"
            "**Cancel a download**\n"
            "Copy the job ID shown by `/stats`, then send:\n"
            "`/cancel job_id`\n\n"
            "**Upload destination**\n"
            "Saved Messages is used by default. Show the current destination:\n"
            "`/dump`\n\n"
            "Set a channel where completed files should be uploaded:\n"
            "`/dump -1001234567890`\n"
            "You must be the channel owner or an admin who can post.\n"
            "Return uploads to Saved Messages:\n"
            "`/dump off`\n\n"
            "**Important**\n"
            "This Telegram account must be able to open the source message. "
            "Deleted, protected, or inaccessible messages cannot be downloaded."
        )
        sent = await self.client.send_message("me", text, disable_web_page_preview=True)
        with suppress(RPCError):
            await self.client.delete_messages(request.chat.id, [request.id])

    async def _dump(self, request: Message) -> None:
        parts = (request.text or "").strip().split(maxsplit=1)
        if len(parts) != 2:
            current = str(self.dump_chat_id) if self.dump_chat_id is not None else "Saved Messages"
            await self._temporary_notice(
                f"Current upload destination: `{current}`\n\nUsage: `/dump <channel_id>` or `/dump off`",
                [request.id],
                delay=10,
            )
            return
        value = parts[1].strip().lower()
        if value in {"off", "default", "me"}:
            self.dump_chat_id = None
            await self.database.set_dump_chat_id(None)
            await self._temporary_notice("Upload destination reset to Saved Messages.", [request.id], delay=5)
            return
        try:
            chat_id = int(value)
            chat = await self.client.get_chat(chat_id)
            if chat.type != ChatType.CHANNEL:
                raise ValueError("The ID must belong to a Telegram channel")
            member = await self.client.get_chat_member(chat_id, "me")
            if member.status == ChatMemberStatus.OWNER:
                can_post = True
            elif member.status == ChatMemberStatus.ADMINISTRATOR:
                privileges = getattr(member, "privileges", None)
                can_post = bool(privileges and privileges.can_post_messages)
            else:
                can_post = False
            if not can_post:
                raise ValueError("You must be the channel owner or an admin with permission to post")
        except ValueError as exc:
            await self._temporary_notice(str(exc), [request.id], delay=10)
            return
        except RPCError:
            await self._temporary_notice(
                "Could not access that channel. Check the ID and your channel permissions.",
                [request.id],
                delay=10,
            )
            return
        self.dump_chat_id = chat_id
        await self.database.set_dump_chat_id(chat_id)
        title = getattr(chat, "title", None) or str(chat_id)
        await self._temporary_notice(f"Upload destination set to **{title}** (`{chat_id}`).", [request.id], delay=5)
        log.info("Dump destination updated to chat %s (%s)", chat_id, title)

    async def _start(self, request: Message) -> None:
        elapsed = max(0, int(time.monotonic() - self.started_at))
        if elapsed < 60:
            amount, unit = elapsed, "second"
        elif elapsed < 3600:
            amount, unit = elapsed // 60, "minute"
        elif elapsed < 86400:
            amount, unit = elapsed // 3600, "hour"
        else:
            amount, unit = elapsed // 86400, "day"
        suffix = "" if amount == 1 else "s"
        await self.client.send_message("me", f"I'm active since {amount} {unit}{suffix}.")
        with suppress(RPCError):
            await self.client.delete_messages(request.chat.id, [request.id])

    async def _accept_link(self, message: Message) -> None:
        parts = (message.text or message.caption or "").strip().split(maxsplit=1)
        try:
            if len(parts) != 2:
                raise InvalidLink("Usage: /get_link <Telegram message link>")
            url = extract_telegram_url(parts[1])
            ref = parse_telegram_link(url)
        except InvalidLink as exc:
            await self._temporary_notice(str(exc), [message.id], message.chat.id)
            return

        job = Job(
            message.id,
            url,
            input_chat_id=message.chat.id,
            source_chat=ref.chat,
            source_message_id=ref.message_id,
        )
        if not await self.database.create(job):
            await self._temporary_notice("This request was already processed.", [message.id], message.chat.id)
            return
        self.jobs[job.id] = job
        await self.queue.put(job)
        with suppress(RPCError):
            await self.client.delete_messages(message.chat.id, [message.id])

    async def _accept(self, message: Message) -> None:
        replied_is_link = False
        try:
            replied = message.reply_to_message
            # Telegram updates do not always include the full replied-to message.
            # Pyrofork can expose only reply_to_message_id, notably for updates in
            # Saved Messages, so hydrate it before deciding that /get has no target.
            if replied is None:
                reply_id = getattr(message, "reply_to_message_id", None)
                if reply_id:
                    replied = await self.client.get_messages(message.chat.id, reply_id)
                    if replied and getattr(replied, "empty", False):
                        replied = None
            if replied is None:
                raise InvalidLink("Reply to the Telegram message you want, then send /get")
            try:
                url = extract_telegram_url(replied.text or replied.caption or "")
                ref = parse_telegram_link(url)
                source_chat, source_message_id = ref.chat, ref.message_id
                replied_is_link = True
            except InvalidLink:
                source_chat, source_message_id = replied.chat.id, replied.id
                url = replied.link or self._message_reference(replied)
        except InvalidLink as exc:
            await self._temporary_notice(str(exc), [message.id], message.chat.id)
            return
        job = Job(message.id, url, input_chat_id=message.chat.id, source_chat=source_chat, source_message_id=source_message_id)
        if not await self.database.create(job):
            await self._temporary_notice("This request was already processed.", [message.id], message.chat.id)
            return
        self.jobs[job.id] = job
        await self.queue.put(job)
        with suppress(RPCError):
            # A directly replied-to media message is the download source and must
            # remain until the worker retrieves it. Only pasted-link messages are
            # safe to remove immediately alongside the command.
            ids = [message.id, replied.id] if message.chat.id == self.me_id and replied_is_link else [message.id]
            await self.client.delete_messages(message.chat.id, ids)

    async def _worker(self, number: int) -> None:
        while True:
            job = await self.queue.get()
            try:
                self._raise_if_cancelled(job)
                await self._process(job)
            except asyncio.CancelledError:
                if not job.cancel_event.is_set():
                    raise
                job.status = JobStatus.CANCELLED
                await self.database.update(job)
            except Exception as exc:
                log.exception("Job %s failed", job.id)
                job.status, job.error = JobStatus.FAILED, self._safe_error(exc)
                await self.database.update(job)
                await self._send_failure(job)
            finally:
                self.queue.task_done()

    async def _process(self, job: Job) -> None:
        self._raise_if_cancelled(job)
        job.status = JobStatus.RESOLVING
        source = await self.client.get_messages(job.source_chat, job.source_message_id)
        self._raise_if_cancelled(job)
        if not source or source.empty:
            raise RuntimeError("The source message does not exist or is inaccessible")
        messages = await self.client.get_media_group(job.source_chat, job.source_message_id) if source.media_group_id else [source]
        if not messages:
            raise RuntimeError("The source message does not contain retrievable content")
        output_ids: list[int] = []
        if all(not item.media for item in messages):
            text = self._with_source(source.text or source.caption or "", job.source_url)
            sent = await self.client.send_message(self.output_chat_id, text, parse_mode=None)
            output_ids.append(sent.id)
        else:
            job_dir = self.settings.temp_dir / f"job_{job.id}"
            job_dir.mkdir(parents=True, exist_ok=False)
            try:
                paths: list[tuple[Message, Path]] = []
                async with self.download_slots:
                    job.status = JobStatus.DOWNLOADING
                    for item in messages:
                        self._raise_if_cancelled(job)
                        expected = self._media_size(item)
                        free = shutil.disk_usage(job_dir).free
                        if expected and free < expected + 256 * 1024 * 1024:
                            raise RuntimeError(f"Insufficient temporary disk space (need at least {human_bytes(expected)})")
                        path = await self.client.download_media(item, file_name=str(job_dir) + "/", progress=job.progress)
                        if not path:
                            raise RuntimeError("Telegram returned no downloadable file")
                        paths.append((item, Path(path)))
                self._raise_if_cancelled(job)
                async with self.upload_slots:
                    job.status = JobStatus.UPLOADING
                    if len(paths) > 1 and all(path.stat().st_size <= self.max_part_size for _, path in paths):
                        output_ids.extend(await self._send_album(paths, job))
                    else:
                        for item, path in paths:
                            self._raise_if_cancelled(job)
                            output_ids.extend(await self._send_one(item, path, job))
            finally:
                shutil.rmtree(job_dir, ignore_errors=True)
        self._raise_if_cancelled(job)
        job.status = JobStatus.COMPLETED
        await self.database.delete(job.id)
        self.jobs.pop(job.id, None)

    async def _send_one(self, source: Message, path: Path, job: Job) -> list[int]:
        files = [path]
        manifest: Path | None = None
        if path.stat().st_size > self.max_part_size:
            job.status = JobStatus.SPLITTING
            files, manifest = await asyncio.to_thread(split_file, path, self.max_part_size, job.progress)
            job.status = JobStatus.UPLOADING
        caption = self._with_source(source.caption or "", job.source_url)
        sent_ids: list[int] = []
        if manifest:
            files.append(manifest)
        for index, file in enumerate(files):
            if len(files) > 1:
                part_caption = f"Part {index + 1}/{len(files) - (1 if manifest else 0)}\n{caption}" if file != manifest else f"Reassembly manifest\nSource: {job.source_url}"
                sent = await self.client.send_document(self.output_chat_id, file, caption=part_caption[:1024], progress=job.progress)
            else:
                sent = await self._send_typed(source, file, caption, job)
            sent_ids.append(sent.id)
        return sent_ids

    async def _send_typed(self, source: Message, path: Path, caption: str, job: Job) -> Message:
        kwargs = {"chat_id": self.output_chat_id, "caption": caption[:1024], "parse_mode": None, "progress": job.progress}
        if source.photo:
            return await self.client.send_photo(photo=path, **kwargs)
        if source.video:
            return await self.client.send_video(video=path, supports_streaming=True, **kwargs)
        if source.audio:
            return await self.client.send_audio(audio=path, title=source.audio.title, performer=source.audio.performer, **kwargs)
        if source.voice:
            return await self.client.send_voice(voice=path, **kwargs)
        if source.animation:
            return await self.client.send_animation(animation=path, **kwargs)
        if source.video_note:
            kwargs.pop("caption")
            kwargs.pop("parse_mode")
            return await self.client.send_video_note(video_note=path, **kwargs)
        if source.sticker:
            kwargs.pop("caption")
            kwargs.pop("parse_mode")
            return await self.client.send_sticker(sticker=path, **kwargs)
        return await self.client.send_document(document=path, file_name=getattr(source.document, "file_name", None), **kwargs)

    async def _send_album(self, paths: list[tuple[Message, Path]], job: Job) -> list[int]:
        media = []
        for index, (source, path) in enumerate(paths):
            caption = self._with_source(source.caption or "", job.source_url) if index == 0 else ""
            if source.photo:
                media.append(InputMediaPhoto(path, caption=caption))
            elif source.video:
                media.append(InputMediaVideo(path, caption=caption, supports_streaming=True))
            elif source.audio:
                media.append(InputMediaAudio(path, caption=caption))
            else:
                media.append(InputMediaDocument(path, caption=caption))
        sent = await self.client.send_media_group(self.output_chat_id, media)
        return [item.id for item in sent]

    async def _stats(self, request: Message) -> None:
        parts = (request.text or "").split(maxsplit=1)
        try:
            page = int(parts[1]) if len(parts) == 2 else 1
            if page < 1:
                raise ValueError
        except ValueError:
            await self._temporary_notice("Usage: /stats [page number]", [request.id])
            return

        page_size = 10
        active = sorted(
            (job for job in self.jobs.values() if job.status not in TERMINAL),
            key=lambda value: value.created_at,
        )
        total_pages = max(1, (len(active) + page_size - 1) // page_size)
        if page > total_pages:
            await self._temporary_notice(f"Page {page} does not exist. Last page: {total_pages}.", [request.id])
            return

        start = (page - 1) * page_size
        lines = [
            f"Active jobs: {len(active)} | Queued: {self.queue.qsize()}",
            f"Page {page}/{total_pages}",
        ]
        for job in active[start : start + page_size]:
            detail = f"{job.percent:.1f}% · {human_bytes(job.current)}/{human_bytes(job.total)} · {human_bytes(job.speed)}/s · ETA {human_duration(job.eta)}" if job.total else "waiting"
            lines.append(f"\n`{job.id}` · {job.status}\n{detail}")
        if not active:
            lines.append("\nNo queued or active jobs.")
        response = await self.client.send_message("me", "\n".join(lines))
        self._background(self._delete_later([request.id, response.id], delay=5))

    async def _cancel(self, request: Message) -> None:
        parts = (request.text or "").split(maxsplit=1)
        job = self.jobs.get(parts[1].strip()) if len(parts) == 2 else None
        if not job or job.status in TERMINAL:
            await self._temporary_notice("Usage: /cancel <active_job_id>", [request.id])
            return
        job.cancel_event.set()
        job.status = JobStatus.CANCELLED
        await self.database.update(job)
        await self._temporary_notice(f"Cancelled job `{job.id}`.", [request.id], delay=5)

    async def _send_failure(self, job: Job) -> None:
        await self.client.send_message("me", f"Could not retrieve this post.\n\nReason: {job.error}\nSource: {job.source_url}", disable_web_page_preview=True)

    async def _temporary_notice(
        self,
        text: str,
        delete_ids: list[int],
        chat_id: int | str = "me",
        delay: int | None = None,
    ) -> None:
        sent = await self.client.send_message(chat_id, text)
        self._background(self._delete_later([*delete_ids, sent.id], chat_id, delay))

    async def _delete_later(
        self,
        ids: list[int],
        chat_id: int | str = "me",
        delay: int | None = None,
    ) -> None:
        await asyncio.sleep(self.settings.stats_ttl_seconds if delay is None else delay)
        with suppress(RPCError):
            await self.client.delete_messages(chat_id, ids)

    def _background(self, coro) -> None:
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    @property
    def output_chat_id(self) -> int | str:
        return self.dump_chat_id if self.dump_chat_id is not None else "me"

    @staticmethod
    def _raise_if_cancelled(job: Job) -> None:
        if job.cancel_event.is_set():
            raise asyncio.CancelledError

    async def _cleanup_abandoned(self) -> None:
        self.settings.temp_dir.mkdir(parents=True, exist_ok=True)
        for child in self.settings.temp_dir.glob("job_*"):
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    @staticmethod
    def _with_source(text: str, url: str) -> str:
        separator = "\n\n" if text else ""
        return f"{text}{separator}Source: {url}"

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        if isinstance(exc, FloodWait):
            return "Telegram requested a temporary pause; please retry later"
        if isinstance(exc, RPCError):
            return "Telegram denied or could not complete the request"
        return str(exc)[:300] or "Unexpected processing failure"

    @staticmethod
    def _media_size(message: Message) -> int:
        media = next((getattr(message, name, None) for name in ("document", "video", "audio", "voice", "animation", "video_note", "sticker", "photo") if getattr(message, name, None)), None)
        return int(getattr(media, "file_size", 0) or 0)

    @staticmethod
    def _message_reference(message: Message) -> str:
        chat_id = int(message.chat.id)
        if chat_id < -1000000000000:
            internal = abs(chat_id) - 1000000000000
            return f"https://t.me/c/{internal}/{message.id}"
        return "Telegram message (no shareable source link)"
