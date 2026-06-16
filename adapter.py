"""
Max Messenger platform adapter for Hermes Gateway.

Uses long-polling (GET /updates) for receiving messages and
REST API (POST /messages) for sending.

API docs: https://dev.max.ru/docs-api
Base URL: https://platform-api.max.ru
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform, PlatformConfig

logger = logging.getLogger(__name__)

MAX_API = "https://platform-api.max.ru"
MAX_ATTACHMENT_TYPES = {"image", "file", "voice", "video", "audio", "contact", "inline_keyboard", "clipboard", "location"}
MAX_NATIVE_COMMANDS = [
    {"name": "help", "description": "Show available commands."},
    {"name": "commands", "description": "List all slash commands."},
    {"name": "status", "description": "Show current status."},
    {"name": "whoami", "description": "Show your sender id."},
    {"name": "model", "description": "Show or set the model."},
    {"name": "reset", "description": "Reset the current session."},
    {"name": "new", "description": "Start a new session."},
    {"name": "think", "description": "Set thinking level."},
    {"name": "verbose", "description": "Toggle verbose mode."},
    {"name": "reasoning", "description": "Toggle reasoning visibility."},
    {"name": "usage", "description": "Usage footer or cost summary."},
    {"name": "stop", "description": "Stop the current run."},
]

MAX_ATTACHMENT_MESSAGE_TYPES = {
    "image": MessageType.PHOTO,
    "photo": MessageType.PHOTO,
    "voice": MessageType.VOICE,
    "audio": MessageType.AUDIO,
    "video": MessageType.VIDEO,
    "file": MessageType.DOCUMENT,
    "document": MessageType.DOCUMENT,
    "location": MessageType.LOCATION,
}

MAX_ATTACHMENT_MEDIA_TYPES = {
    "image": "image/jpeg",
    "photo": "image/jpeg",
    "voice": "audio/ogg",
    "audio": "audio/mpeg",
    "video": "video/mp4",
    "file": "application/octet-stream",
    "document": "application/octet-stream",
}
MAX_INBOUND_MEDIA_MAX_BYTES = 50 * 1024 * 1024


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[Any, Any] = {}
        for key, nested in value.items():
            if str(key).lower() in {"token", "access_token", "authorization"}:
                redacted[key] = "<redacted>"
            else:
                redacted[key] = _redact_sensitive(nested)
        return redacted
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value]
    return value


def _json_preview(value: Any, limit: int = 4000) -> str:
    try:
        text = json.dumps(_redact_sensitive(value), default=str, ensure_ascii=False)
    except Exception:
        text = str(value)
    return text if len(text) <= limit else f"{text[:limit]}...<truncated>"


def _max_attachment_type(attachment: Any) -> str:
    if not isinstance(attachment, dict):
        return ""
    return str(attachment.get("type") or attachment.get("attachment_type") or "").lower()


def _max_message_type(text: str, attachments: list[Any]) -> MessageType:
    if not attachments:
        return MessageType.TEXT
    for attachment in attachments:
        attachment_type = _max_attachment_type(attachment)
        if attachment_type in MAX_ATTACHMENT_MESSAGE_TYPES:
            return MAX_ATTACHMENT_MESSAGE_TYPES[attachment_type]
    return MessageType.TEXT if text else MessageType.DOCUMENT


def _first_http_url(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value if value.startswith(("http://", "https://")) else None
    if isinstance(value, dict):
        for key in ("url", "file_url", "download_url", "link", "src", "href"):
            found = _first_http_url(value.get(key))
            if found:
                return found
        for nested in value.values():
            found = _first_http_url(nested)
            if found:
                return found
    if isinstance(value, list):
        for item in value:
            found = _first_http_url(item)
            if found:
                return found
    return None


def _max_attachment_media(attachments: list[Any]) -> tuple[list[str], list[str]]:
    media_urls: list[str] = []
    media_types: list[str] = []
    for attachment in attachments:
        url = _first_http_url(attachment)
        if not url:
            continue
        attachment_type = _max_attachment_type(attachment)
        media_urls.append(url)
        media_types.append(MAX_ATTACHMENT_MEDIA_TYPES.get(attachment_type, "application/octet-stream"))
    return media_urls, media_types


def _max_attachment_summary(attachments: list[Any]) -> str:
    if not attachments:
        return ""
    counts: dict[str, int] = {}
    for attachment in attachments:
        attachment_type = _max_attachment_type(attachment) or "unknown"
        counts[attachment_type] = counts.get(attachment_type, 0) + 1
    parts = [f"{count} {attachment_type}" for attachment_type, count in sorted(counts.items())]
    return "[MAX attachment: " + ", ".join(parts) + "]"


def _max_attachments_from_candidate(candidate: Any) -> list[Any]:
    if isinstance(candidate, list):
        return candidate
    if isinstance(candidate, dict):
        if "type" in candidate or "payload" in candidate:
            return [candidate]
        for key in ("attachments", "attachment"):
            value = candidate.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                return [value]
    return []


def _max_extract_attachments(update: dict, message: dict, body: dict) -> tuple[list[Any], str]:
    """Extract inbound MAX message attachments from known compatible shapes."""
    candidates = (
        ("message.body.attachments", body.get("attachments")),
        ("message.attachments", message.get("attachments")),
        ("update.attachments", update.get("attachments")),
        ("message.body.attachment", body.get("attachment")),
        ("message.attachment", message.get("attachment")),
        ("update.attachment", update.get("attachment")),
    )
    empty_source = ""
    for source, candidate in candidates:
        attachments = _max_attachments_from_candidate(candidate)
        if attachments:
            return attachments, source
        if isinstance(candidate, list) and not empty_source:
            empty_source = source
    return [], empty_source or "none"


class MaxAdapter(BasePlatformAdapter):
    """Long-polling adapter for Max Messenger."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("max"))
        extra = config.extra or {}
        # 1. env var → 2. config token → 3. extra.token
        self.token = os.getenv("MAX_BOT_TOKEN") or getattr(config, "token", None) or extra.get("token", "")
        self._session: Optional[aiohttp.ClientSession] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._last_event_id: int = extra.get("last_event_id", 0)
        self._running = False
        self._known_message_events: set = set()  # Dedup by message id + event payload
        self._last_chat_action_at: Dict[tuple[str, str], float] = {}

    # ── connection ──────────────────────────────────────────────

    async def connect(self) -> bool:
        if not self.token:
            logger.error("Max: MAX_BOT_TOKEN not set")
            return False

        self._session = aiohttp.ClientSession(
            headers={
                "Authorization": self.token,
            },
            timeout=aiohttp.ClientTimeout(total=120),
        )

        # Verify token by polling (no dedicated /me endpoint)
        try:
            async with self._session.get(
                f"{MAX_API}/updates", params={"limit": 1, "timeout": 5}
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Max: /updates returned {resp.status}")
                    await self._session.close()
                    self._session = None
                    return False
                data = await resp.json()
                logger.info(f"Max: connected OK (marker={data.get('marker', '?')})")
        except Exception as e:
            logger.error(f"Max: token check failed: {e}")
            await self._session.close()
            self._session = None
            return False

        await self._register_native_commands()

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._session:
            await self._session.close()
            self._session = None
        self._mark_disconnected()

    # ── message sending ─────────────────────────────────────────

    async def _api_post(self, path: str, params: dict = None, json_body: dict = None) -> dict:
        """Helper: POST to Max API, return parsed JSON or {}."""
        if not self._session:
            return {}
        try:
            async with self._session.post(f"{MAX_API}{path}", params=params, json=json_body) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    if text.strip():
                        return json.loads(text)
                else:
                    body = await resp.text()
                    logger.warning(f"Max API POST {path} → {resp.status}: {body[:300]}")
        except Exception as e:
            logger.warning(f"Max API POST {path} error: {e}")
        return {}

    async def _api_patch(self, path: str, json_body: dict = None) -> dict:
        """Helper: PATCH to Max API, return parsed JSON or {}."""
        if not self._session:
            return {}
        try:
            async with self._session.patch(f"{MAX_API}{path}", json=json_body) as resp:
                text = await resp.text()
                if resp.status == 200:
                    return json.loads(text) if text.strip() else {}
                logger.warning(f"Max API PATCH {path} → {resp.status}: {text[:300]}")
        except Exception as e:
            logger.warning(f"Max API PATCH {path} error: {e}")
        return {}

    async def _register_native_commands(self) -> None:
        """Register MAX native command menu entries.

        MAX exposes the slash-command menu from the bot's ``/me.commands`` field,
        updated with ``PATCH /me``. Keep this list compact: MAX caps it at 32.
        """
        result = await self._api_patch("/me", {"commands": MAX_NATIVE_COMMANDS})
        if result:
            logger.info("Max: native commands registered (%s)", len(MAX_NATIVE_COMMANDS))

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        if not self._session:
            return SendResult(success=False, error="No HTTP session (disconnected)")

        if not content:
            return SendResult(success=False, error="Empty content")

        # Handle markdown → Max format (Max supports markdown-like formatting)
        # MAX rejects messages at its hard 4000-char boundary in some cases.
        text = str(content)[:3900]

        body: Dict[str, Any] = {"text": text, "format": "markdown"}
        if reply_to:
            body["reply_to"] = reply_to

        result = await self._api_post("/messages", params={"chat_id": chat_id}, json_body=body)
        if result:
            msg_id = str(result.get("message_id") or result.get("message", {}).get("body", {}).get("mid", ""))
            return SendResult(success=True, message_id=msg_id)
        return SendResult(success=False, error="API returned empty response")

    async def _send_chat_action(self, chat_id, action: str, *, debounce_seconds: float = 0.0) -> bool:
        """Send a MAX chat action such as typing_on or mark_seen."""
        chat_id_str = str(chat_id or "").strip()
        if not chat_id_str:
            return False

        if debounce_seconds > 0:
            key = (chat_id_str, action)
            now = time.monotonic()
            last = self._last_chat_action_at.get(key, 0.0)
            if now - last < debounce_seconds:
                return True
            self._last_chat_action_at[key] = now

        result = await self._api_post(
            f"/chats/{chat_id_str}/actions",
            json_body={"action": action},
        )
        return bool(result)

    async def send_typing(self, chat_id, metadata=None) -> None:
        """Show the MAX typing status while Hermes is processing a turn."""
        await self._send_chat_action(chat_id, "typing_on", debounce_seconds=1.5)

    async def mark_seen(self, chat_id) -> None:
        """Mark incoming MAX messages as seen/read."""
        await self._send_chat_action(chat_id, "mark_seen")

    def _inbound_media_cache_dir(self) -> Path:
        root = Path(os.getenv("HERMES_HOME") or Path.home() / ".hermes")
        cache_dir = root / "cache" / "max" / "inbound"
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    @staticmethod
    def _extension_for_content_type(content_type: str, attachment_type: str) -> str:
        normalized = (content_type or "").split(";", 1)[0].strip().lower()
        return {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "image/webp": ".webp",
            "image/bmp": ".bmp",
            "video/mp4": ".mp4",
            "audio/ogg": ".ogg",
            "audio/mpeg": ".mp3",
        }.get(normalized, ".jpg" if attachment_type in {"image", "photo"} else ".bin")

    async def _download_inbound_media(
        self,
        *,
        url: str,
        attachment_type: str,
        message_id: str,
        index: int,
    ) -> tuple[Optional[str], Optional[str]]:
        if not url.startswith(("http://", "https://")):
            return None, None

        digest = hashlib.sha256(f"{message_id}:{index}:{url}".encode("utf-8")).hexdigest()[:16]
        safe_mid = re.sub(r"[^A-Za-z0-9_.-]+", "_", message_id or "message")[:80]
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            # Use a clean session so MAX bot Authorization is never sent to the CDN host.
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        logger.warning("Max inbound media download failed: HTTP %s", resp.status)
                        return None, None
                    content_type = resp.headers.get("content-type", "")
                    ext = self._extension_for_content_type(content_type, attachment_type)
                    path = self._inbound_media_cache_dir() / f"{safe_mid}_{index}_{digest}{ext}"

                    total = 0
                    with path.open("wb") as f:
                        async for chunk in resp.content.iter_chunked(64 * 1024):
                            total += len(chunk)
                            if total > MAX_INBOUND_MEDIA_MAX_BYTES:
                                f.close()
                                try:
                                    path.unlink(missing_ok=True)
                                except Exception:
                                    pass
                                logger.warning("Max inbound media too large: >%s bytes", MAX_INBOUND_MEDIA_MAX_BYTES)
                                return None, None
                            f.write(chunk)

            logger.info("Max: cached inbound %s media: %s", attachment_type, path)
            return str(path), content_type.split(";", 1)[0].strip().lower() or None
        except Exception as exc:
            logger.warning("Max inbound media download error: %s", exc)
            return None, None

    async def _resolve_inbound_media(self, attachments: list[Any], message_id: str) -> tuple[list[str], list[str]]:
        media_urls: list[str] = []
        media_types: list[str] = []
        for index, attachment in enumerate(attachments):
            url = _first_http_url(attachment)
            if not url:
                continue
            attachment_type = _max_attachment_type(attachment)
            fallback_type = MAX_ATTACHMENT_MEDIA_TYPES.get(attachment_type, "application/octet-stream")
            if attachment_type in {"image", "photo"}:
                cached_path, downloaded_type = await self._download_inbound_media(
                    url=url,
                    attachment_type=attachment_type,
                    message_id=message_id,
                    index=index,
                )
                if cached_path:
                    media_urls.append(cached_path)
                    media_types.append(downloaded_type or fallback_type)
                    continue
            media_urls.append(url)
            media_types.append(fallback_type)
        return media_urls, media_types

    async def _upload_file(self, file_path: str, file_type: str = "file") -> Optional[dict]:
        """Upload a file to Max and return the attachment payload."""
        if not self._session:
            return None

        path = Path(file_path)
        if not path.exists():
            logger.warning(f"Max upload: file not found: {file_path}")
            return None
        upload_type = "audio" if file_type == "voice" else file_type

        try:
            headers = {"Authorization": self.token}
            async with self._session.post(
                f"{MAX_API}/uploads",
                params={"type": upload_type},
                headers=headers,
            ) as resp:
                if resp.status == 200:
                    upload_init = await resp.json()
                else:
                    body = await resp.text()
                    logger.warning(f"Max upload init → {resp.status}: {body[:300]}")
                    return None

            upload_url = upload_init.get("url")
            if not upload_url:
                logger.warning("Max upload init returned no url: %s", upload_init)
                return None

            data = aiohttp.FormData()
            with path.open("rb") as f:
                data.add_field("data", f, filename=path.name)
                async with self._session.post(upload_url, data=data) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        logger.warning(f"Max upload data → {resp.status}: {text[:300]}")
                        return None
                    upload_result = json.loads(text) if text.strip() else {}

            payload = upload_result if isinstance(upload_result, dict) else {}
            if not payload.get("token") and upload_init.get("token"):
                payload["token"] = upload_init["token"]
            if payload:
                logger.info("Max: uploaded %s → payload keys=%s", file_path, sorted(payload.keys()))
                return payload
            logger.warning("Max upload data returned empty payload for %s", file_path)
        except Exception as e:
            logger.warning(f"Max upload error: {e}")
        return None

    async def _send_attachment_message(
        self,
        chat_id,
        attachment_type: str,
        payload: dict,
        caption: Optional[str] = None,
        *,
        max_attempts: int = 3,
    ) -> SendResult:
        body = {
            "text": caption or "",
            "format": "markdown" if caption else None,
            "attachments": [{"type": attachment_type, "payload": payload}],
        }
        if body["format"] is None:
            body.pop("format")

        for attempt in range(max_attempts):
            result = await self._api_post("/messages", params={"chat_id": chat_id}, json_body=body)
            if result:
                return SendResult(success=True, message_id=str(result.get("message_id", "")))
            if attempt < max_attempts - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
        return SendResult(success=False, error="Max attachment message failed")

    async def send_image(self, chat_id, image_url, caption=None) -> SendResult:
        return await self._send_media(chat_id, image_url, "image", caption)

    async def send_image_file(
        self,
        chat_id,
        image_path=None,
        caption=None,
        reply_to=None,
        metadata=None,
        **kwargs,
    ) -> SendResult:
        """Send image from local file path."""
        path = image_path or kwargs.get("path")
        if not path:
            return SendResult(success=False, error="image_path is required")

        payload = await self._upload_file(path, "image")
        if not payload:
            return SendResult(success=False, error="Max image upload failed")
        return await self._send_attachment_message(chat_id, "image", payload, caption)

    async def send_document(
        self,
        chat_id,
        file_path=None,
        caption=None,
        reply_to=None,
        metadata=None,
        **kwargs,
    ) -> SendResult:
        path = file_path or kwargs.get("path")
        if not path:
            return SendResult(success=False, error="file_path is required")

        payload = await self._upload_file(path, "file")
        if not payload:
            return SendResult(success=False, error="Max file upload failed")
        return await self._send_attachment_message(chat_id, "file", payload, caption)

    async def send_voice(self, chat_id, path) -> SendResult:
        payload = await self._upload_file(path, "voice")
        if not payload:
            return SendResult(success=False, error="Max voice upload failed")
        return await self._send_attachment_message(chat_id, "audio", payload)

    async def send_video(self, chat_id, path, caption=None) -> SendResult:
        payload = await self._upload_file(path, "video")
        if not payload:
            return SendResult(success=False, error="Max video upload failed")
        return await self._send_attachment_message(chat_id, "video", payload, caption)

    async def _send_media(self, chat_id, url: str, media_type: str, caption=None) -> SendResult:
        """Send media by URL — Max may not support URL-based images directly."""
        # Fall back: try sending as text with URL
        text = caption or ""
        if url and media_type == "image":
            body = {
                "text": text,
                "format": "markdown" if text else None,
                "attachments": [{"type": "image", "payload": {"url": url}}],
            }
            if body["format"] is None:
                body.pop("format")
            result = await self._api_post("/messages", params={"chat_id": chat_id}, json_body=body)
            if result:
                return SendResult(success=True, message_id=str(result.get("message_id", "")))

        # Plain text fallback
        return await self.send(chat_id, f"{text}\n{url}" if url else text)

    # ── chat info ───────────────────────────────────────────────

    async def get_chat_info(self, chat_id) -> dict:
        if not self._session:
            return {"name": str(chat_id), "type": "dm"}

        try:
            async with self._session.get(f"{MAX_API}/chats/{chat_id}") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    chat = data.get("chat", data)
                    return {
                        "name": chat.get("title") or chat.get("name") or f"chat_{chat_id}",
                        "type": "group" if chat.get("is_group") or chat.get("type") == "group" else "dm",
                        "chat_id": chat_id,
                    }
        except Exception:
            pass
        return {"name": f"chat_{chat_id}", "type": "dm"}

    # ── long-polling loop ───────────────────────────────────────

    async def _poll_loop(self):
        """Long-poll GET /updates with marker."""
        logger.warning("Max: _poll_loop STARTING")
        marker: int = 0
        backoff = 1
        while self._running and self._session:
            try:
                params = {
                    "limit": 100,
                    "timeout": 30,
                }
                if marker > 0:
                    params["marker"] = marker

                async with self._session.get(
                    f"{MAX_API}/updates", params=params, timeout=aiohttp.ClientTimeout(total=40)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Max poll: HTTP {resp.status}")
                        await asyncio.sleep(min(backoff, 30))
                        backoff = min(backoff * 2, 30)
                        continue

                    body = await resp.json()
                    backoff = 1
                    updates = body.get("updates", [])
                    
                    new_marker = body.get("marker")
                    updates_count = len(updates) if isinstance(updates, list) else "?"
                    logger.warning(
                        "Max poll: marker=%s new_marker=%s updates_count=%s",
                        marker,
                        new_marker,
                        updates_count,
                    )
                    if isinstance(updates, list) and not updates and new_marker not in (None, marker):
                        logger.warning("Max poll: marker advanced with zero returned updates")
                    if updates and isinstance(updates, list) and len(updates) > 0:
                        logger.warning("Max poll: first update sample: %s", _json_preview(updates[0]))
                    
                    if not isinstance(updates, list):
                        logger.warning(f"Max poll: unexpected format: {type(updates)}")
                        continue

                    if new_marker is not None:
                        marker = new_marker

                    for u in updates:
                        if not isinstance(u, dict):
                            continue
                        logger.warning(f"Max: about to call _handle_update, update_type={u.get('update_type','?')}")
                        await self._handle_update(u)

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Max poll error: {e}")
                await asyncio.sleep(min(backoff, 30))
                backoff = min(backoff * 2, 30)

    async def _handle_update(self, u: dict):
        """Parse a Max update (from /updates) and forward as MessageEvent."""
        update_type = u.get("update_type", "")
        logger.warning(f"Max: _handle_update type={update_type} keys={list(u.keys())}")

        # Bot started event
        if update_type == "bot_started":
            chat_id = u.get("chat_id")
            if chat_id:
                logger.info(f"Max: bot_started in chat {chat_id}")
            return

        # Callback (inline button press)
        if update_type == "message_callback":
            callback = u.get("callback", {}) or {}
            payload = callback.get("payload", "")
            msg = u.get("message", {}) or {}
            chat_id = str(msg.get("chat_id") or u.get("chat_id", ""))
            author = msg.get("author", msg.get("from", {}))
            author_id = str(author.get("user_id", ""))

            source = self.build_source(
                chat_id=chat_id,
                user_id=author_id,
            )

            event = MessageEvent(
                message_id=str(u.get("update_id", u.get("event_id", ""))),
                text=f"/callback {payload}",
                source=source,
                message_type=MessageType.TEXT,
            )
            await self.handle_message(event)
            return

        # Regular message
        msg = u.get("message", {}) or {}
        if not isinstance(msg, dict):
            logger.warning(f"Max: msg is not dict, type={type(msg)}")
            return

        recipient = msg.get("recipient", {}) or {}
        body = msg.get("body", {}) or {}
        chat_id = str(recipient.get("chat_id", ""))
        text = (body.get("text") or "").strip()
        attachments_raw, attachments_source = _max_extract_attachments(u, msg, body)

        logger.warning(
            "Max: parsed chat_id=%s text='%s' attachments=%s source=%s",
            chat_id,
            text[:50],
            len(attachments_raw),
            attachments_source,
        )
        if attachments_raw:
            logger.warning("Max: attachment preview: %s", _json_preview(attachments_raw))

        if not chat_id or (not text and not attachments_raw):
            logger.warning(f"Max: skipping — no chat_id or content")
            return

        message_id = str(body.get("mid") or msg.get("message_id") or u.get("update_id", ""))
        attachment_fingerprint = tuple(
            (
                _max_attachment_type(attachment),
                str(attachment.get("payload", ""))[:120] if isinstance(attachment, dict) else str(attachment)[:120],
            )
            for attachment in attachments_raw
        )
        dedup_key = (message_id, update_type, text, attachment_fingerprint)
        if dedup_key in self._known_message_events:
            logger.warning(f"Max: duplicate event message_id={message_id} type={update_type}, skipping")
            return
        self._known_message_events.add(dedup_key)

        # Housekeeping
        if len(self._known_message_events) > 10000:
            self._known_message_events.clear()

        sender = msg.get("sender") or msg.get("author") or msg.get("from") or {}
        author_id = str(sender.get("user_id", ""))
        author_name = sender.get("first_name") or sender.get("name") or f"user_{author_id}"

        # Skip own messages
        if author_id == "282300124":
            logger.warning(f"Max: skipping own message")
            return

        await self.mark_seen(chat_id)

        logger.warning(f"Max: building event for user={author_name} chat={chat_id} text='{text[:50]}'")

        source = self.build_source(
            chat_id=chat_id,
            user_id=author_id,
            user_name=author_name,
        )

        media_urls, media_types = await self._resolve_inbound_media(attachments_raw, message_id)
        event_text = text
        if attachments_raw:
            summary = _max_attachment_summary(attachments_raw)
            event_text = f"{text}\n\n{summary}".strip() if text else summary

        event = MessageEvent(
            message_id=message_id,
            text=event_text,
            source=source,
            raw_message=u,
            message_type=_max_message_type(text, attachments_raw),
            media_urls=media_urls,
            media_types=media_types,
        )

        logger.warning(f"Max: calling handle_message with event")
        await self.handle_message(event)
        logger.warning(f"Max: handle_message completed")


# ── plugin entry points ─────────────────────────────────────────

def check_requirements() -> bool:
    """Check if Max adapter dependencies are available."""
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        return False


def validate_config(config: PlatformConfig) -> bool:
    """Validate that we have a token."""
    extra = getattr(config, "extra", {}) or {}
    token = os.getenv("MAX_BOT_TOKEN") or getattr(config, "token", None) or extra.get("token", "")
    return bool(token)


def _env_enablement() -> dict:
    """Seed Max config from environment variables."""
    data: dict[str, Any] = {}
    token = os.getenv("MAX_BOT_TOKEN", "").strip()
    if token:
        data["token"] = token
    home = os.getenv("MAX_HOME_CHANNEL", "").strip()
    if home:
        data["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("MAX_HOME_CHANNEL_NAME", "Home"),
            "thread_id": os.getenv("MAX_HOME_CHANNEL_THREAD_ID", "").strip() or None,
        }
    return data


def register(ctx):
    """Register Max Messenger platform adapter."""
    ctx.register_platform(
        name="max",
        label="Max Messenger",
        adapter_factory=lambda cfg: MaxAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        env_enablement_fn=_env_enablement,
        required_env=["MAX_BOT_TOKEN"],
        install_hint="pip install aiohttp",
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        cron_deliver_env_var="MAX_HOME_CHANNEL",
        max_message_length=4000,
        emoji="💎",
        platform_hint=(
            "You are on Max Messenger (max.ru) — российский мессенджер. "
            "Поддерживает markdown: **bold**, *italic*, ~~strikethrough~~, `code`, "
            "[links](url), ## headers. Таблиц нет — используй списки. "
            "Можно отправлять изображения и файлы: чтобы доставить файл пользователю, "
            "добавь в ответ MEDIA:/absolute/path/to/file. Аудиофайлы (.mp3, .wav, .m4a, .ogg) "
            "отправляй именно как файлы/document, а не как voice/audio, чтобы Max не пережимал звук. "
            "Не говори, что отправка файлов недоступна, если файл лежит локально и есть абсолютный путь. "
            "Лимит сообщения: 4000 символов. Общайся на русском."
        ),
    )
