"""
Max Messenger platform adapter for Hermes Gateway.

Uses Webhook (POST /subscriptions) or long-polling (GET /updates) for
receiving messages and REST API (POST /messages) for sending.

API docs: https://dev.max.ru/docs-api
Base URL: https://platform-api.max.ru
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import web

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform, PlatformConfig

logger = logging.getLogger(__name__)

DEFAULT_MAX_API_BASE_URL = "https://platform-api.max.ru"
DEFAULT_WEBHOOK_HOST = "0.0.0.0"
DEFAULT_WEBHOOK_PORT = 8650
DEFAULT_WEBHOOK_PATH = "/max/webhook"
DEFAULT_UPDATE_TYPES = ["message_created", "message_callback", "bot_started"]
MAX_WEBHOOK_MAX_BODY_BYTES = 2 * 1024 * 1024
# MAX documents POST /messages text as "up to 4000 characters". Keep a
# transport margin because the API has rejected boundary-sized markdown text.
MAX_MESSAGE_TEXT_LIMIT = 4000
MAX_MESSAGE_TEXT_CHUNK_SIZE = 3900
MAX_INLINE_KEYBOARD_COLUMNS = 1
MAX_CALLBACK_NOTIFICATION_LIMIT = 1024
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
MAX_BUTTONS_COMMENT_RE = re.compile(
    r"<!--\s*(?:max_buttons|max_inline_keyboard)\s*:\s*(?P<payload>.*?)\s*-->",
    re.IGNORECASE | re.DOTALL,
)
MAX_BUTTONS_FENCE_RE = re.compile(
    r"(?:^|\n)```(?:max_buttons|max_inline_keyboard)\s*\n(?P<payload>.*?)(?:\n```)",
    re.IGNORECASE | re.DOTALL,
)
MAX_NUMBERED_OPTION_RE = re.compile(
    r"^\s*(?:[1-9][\.)]|[1-9]\ufe0f?\u20e3)\s+(?P<text>\S.+?)\s*$"
)


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


def _max_find_first_key(value: Any, names: set[str]) -> str:
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() in names and nested not in (None, ""):
                return str(nested)
        for nested in value.values():
            found = _max_find_first_key(nested, names)
            if found:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _max_find_first_key(nested, names)
            if found:
                return found
    return ""


def _max_callback_payload(update: dict, callback: Any) -> str:
    if isinstance(callback, str):
        return callback.strip()
    if isinstance(callback, dict):
        direct = callback.get("payload") or callback.get("callback_payload") or callback.get("data")
        if direct:
            return str(direct).strip()
        found = _max_find_first_key(callback, {"payload", "callback_payload", "data"})
        if found:
            return found.strip()
    return str(update.get("payload") or update.get("callback_payload") or update.get("data") or "").strip()


def _max_callback_id(update: dict, callback: Any) -> str:
    if isinstance(callback, dict):
        direct = callback.get("callback_id")
        if direct:
            return str(direct).strip()
        found = _max_find_first_key(callback, {"callback_id"})
        if found:
            return found.strip()
    return str(update.get("callback_id") or "").strip()


def _max_callback_text(update: dict, callback: Any) -> str:
    if isinstance(callback, dict):
        found = _max_find_first_key(callback, {"text", "title", "label"})
        if found:
            return found.strip()
    return str(update.get("text") or update.get("title") or update.get("label") or "").strip()


def _max_message_text(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    body = message.get("body")
    if isinstance(body, dict):
        text = body.get("text")
        if text:
            return str(text)
    text = message.get("text")
    return str(text) if text else ""


def _max_trim_callback_text(text: str, limit: int = MAX_CALLBACK_NOTIFICATION_LIMIT) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= limit:
        return value
    return value[: max(1, limit - 1)].rstrip() + "…"


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


def _split_max_text(text: str, limit: int = MAX_MESSAGE_TEXT_CHUNK_SIZE) -> list[str]:
    """Split outbound text into MAX-sized chunks without silently truncating."""
    text = str(text or "")
    if not text:
        return []
    if limit <= 0:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        window = remaining[:limit]
        split_at = -1
        for separator in ("\n\n", "\n", " "):
            pos = window.rfind(separator)
            if pos > 0:
                split_at = pos + len(separator)
                break
        if split_at <= 0:
            split_at = limit

        chunk = remaining[:split_at]
        if not chunk:
            chunk = remaining[:limit]
            split_at = limit
        chunks.append(chunk)
        remaining = remaining[split_at:]

    if remaining:
        chunks.append(remaining)
    return chunks


def _max_button(value: Any) -> Optional[dict[str, Any]]:
    """Normalize a compact button description into a MAX Button object."""
    if not isinstance(value, dict):
        return None

    text = str(value.get("text") or value.get("label") or "").strip()
    if not text:
        return None

    if value.get("type") in {"callback", "link", "message", "clipboard", "open_app"}:
        button = dict(value)
        button["text"] = text
        return button

    if "payload" in value or "callback_data" in value or "data" in value:
        payload = str(value.get("payload") or value.get("callback_data") or value.get("data") or "")
        if payload:
            return {"type": "callback", "text": text, "payload": payload}

    if "url" in value:
        url = str(value.get("url") or "").strip()
        if url:
            return {"type": "link", "text": text, "url": url}

    return None


def _max_inline_keyboard_attachment(buttons: Any) -> Optional[dict[str, Any]]:
    """Build an inline_keyboard attachment from row/button metadata."""
    if not isinstance(buttons, list):
        return None

    rows: list[list[dict[str, Any]]] = []
    for row in buttons:
        candidates = row if isinstance(row, list) else [row]
        normalized_row = [button for item in candidates if (button := _max_button(item))]
        for index in range(0, len(normalized_row), MAX_INLINE_KEYBOARD_COLUMNS):
            chunk = normalized_row[index : index + MAX_INLINE_KEYBOARD_COLUMNS]
            if chunk:
                rows.append(chunk)

    if not rows:
        return None
    return {"type": "inline_keyboard", "payload": {"buttons": rows}}


def _max_metadata_attachments(metadata: Any) -> list[dict[str, Any]]:
    """Extract MAX attachments from send metadata.

    Supported shapes:
    - {"max_inline_keyboard": [[{"text": "Open", "url": "https://..."}]]}
    - {"max_inline_keyboard": [[{"text": "Pick", "payload": "resume:<id>"}]]}
    - {"max_attachments": [{"type": "inline_keyboard", "payload": {...}}]}
    """
    if not isinstance(metadata, dict):
        return []

    attachments: list[dict[str, Any]] = []
    raw_attachments = metadata.get("max_attachments") or metadata.get("attachments")
    if isinstance(raw_attachments, list):
        attachments.extend(item for item in raw_attachments if isinstance(item, dict) and item.get("type"))

    keyboard = (
        metadata.get("max_inline_keyboard")
        or metadata.get("inline_keyboard")
        or metadata.get("max_buttons")
        or metadata.get("buttons")
    )
    keyboard_attachment = _max_inline_keyboard_attachment(keyboard)
    if keyboard_attachment:
        attachments.append(keyboard_attachment)

    return attachments


def _max_buttons_from_directive_payload(payload: str) -> Optional[dict[str, Any]]:
    """Parse a plugin-local inline keyboard directive into a MAX attachment."""
    try:
        value = json.loads(payload)
    except Exception as exc:
        logger.warning("Max: ignored invalid inline keyboard directive: %s", exc)
        return None

    if isinstance(value, dict):
        if value.get("type") == "inline_keyboard":
            attachments = _max_attachments_from_candidate(value)
            for attachment in attachments:
                if _max_attachment_type(attachment) == "inline_keyboard":
                    return attachment
            return None
        value = value.get("buttons") or value.get("max_buttons") or value.get("inline_keyboard")

    return _max_inline_keyboard_attachment(value)


def _max_extract_inline_keyboard_directives(text: str) -> tuple[str, list[dict[str, Any]]]:
    """Remove MAX inline keyboard directives from text and return attachments.

    This is intentionally plugin-local: Hermes core only sends plain text plus
    routing metadata, so the MAX adapter owns the MAX-specific UI hint.
    """
    lowered = text.lower()
    if "max_buttons" not in lowered and "max_inline_keyboard" not in lowered:
        return text, []

    attachments: list[dict[str, Any]] = []

    def replace(match: re.Match) -> str:
        attachment = _max_buttons_from_directive_payload(match.group("payload").strip())
        if attachment:
            attachments.append(attachment)
        return ""

    text = MAX_BUTTONS_COMMENT_RE.sub(replace, text)
    text = MAX_BUTTONS_FENCE_RE.sub(replace, text)
    text = re.sub(
        r"<!--\s*(?:max_buttons|max_inline_keyboard)\s*:.*$",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(
        r"(?:^|\n)```(?:max_buttons|max_inline_keyboard)\s*\n.*$",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text, attachments


def _max_auto_keyboard_from_numbered_options(text: str) -> Optional[dict[str, Any]]:
    """Infer buttons from a trailing numbered choice list when max_buttons is omitted."""
    lines = str(text or "").splitlines()
    options: list[str] = []
    for line in reversed(lines):
        if not line.strip():
            if options:
                break
            continue
        match = MAX_NUMBERED_OPTION_RE.match(line)
        if not match:
            break
        options.append(" ".join(match.group("text").split()))

    options.reverse()
    if len(options) < 2 or len(options) > 6:
        return None

    buttons = [
        [{"type": "callback", "text": option, "payload": option}]
        for option in options
    ]
    return _max_inline_keyboard_attachment(buttons)


class MaxAdapter(BasePlatformAdapter):
    """MAX Messenger adapter supporting webhook and long-polling transports."""

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
        self._clarify_state: dict[str, str] = {}
        self._api_base_url = str(
            os.getenv("MAX_API_BASE_URL")
            or extra.get("api_base_url")
            or extra.get("base_url")
            or DEFAULT_MAX_API_BASE_URL
        ).rstrip("/")
        self._bot_user_id = ""
        configured_transport = (
            os.getenv("MAX_TRANSPORT")
            or os.getenv("MAX_MODE")
            or extra.get("transport")
            or extra.get("mode")
            or ""
        )
        self._webhook_url = str(
            os.getenv("MAX_WEBHOOK_URL") or extra.get("webhook_url") or ""
        ).strip()
        self._transport = str(configured_transport or ("webhook" if self._webhook_url else "polling")).strip().lower()
        self._webhook_secret = str(
            os.getenv("MAX_WEBHOOK_SECRET") or extra.get("webhook_secret") or extra.get("secret") or ""
        ).strip()
        self._webhook_host = str(
            os.getenv("MAX_WEBHOOK_HOST") or extra.get("webhook_host") or DEFAULT_WEBHOOK_HOST
        ).strip()
        self._webhook_port = int(
            os.getenv("MAX_WEBHOOK_PORT") or extra.get("webhook_port") or DEFAULT_WEBHOOK_PORT
        )
        self._webhook_path = self._normalize_path(
            os.getenv("MAX_WEBHOOK_PATH") or extra.get("webhook_path") or DEFAULT_WEBHOOK_PATH
        )
        self._webhook_update_types = self._parse_update_types(
            os.getenv("MAX_UPDATE_TYPES") or extra.get("update_types") or DEFAULT_UPDATE_TYPES
        )
        self._webhook_runner: Optional[web.AppRunner] = None
        self._webhook_tasks: set[asyncio.Task] = set()

    @staticmethod
    def _normalize_path(value: Any) -> str:
        path = str(value or DEFAULT_WEBHOOK_PATH).strip() or DEFAULT_WEBHOOK_PATH
        return path if path.startswith("/") else f"/{path}"

    @staticmethod
    def _parse_update_types(value: Any) -> list[str]:
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",")]
        elif isinstance(value, (list, tuple, set)):
            items = [str(item).strip() for item in value]
        else:
            items = []
        return [item for item in items if item] or list(DEFAULT_UPDATE_TYPES)

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

        # Verify token without consuming updates. MAX requires Authorization header.
        try:
            async with self._session.get(
                f"{self._api_base_url}/me", timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Max: /me returned {resp.status}")
                    await self._session.close()
                    self._session = None
                    return False
                data = await resp.json()
                self._bot_user_id = str(data.get("user_id") or "")
                logger.info("Max: connected OK as %s", data.get("username") or data.get("name") or data.get("user_id") or "?")
        except Exception as e:
            logger.error(f"Max: token check failed: {e}")
            await self._session.close()
            self._session = None
            return False

        await self._register_native_commands()

        self._running = True
        if self._transport == "webhook":
            if not self._webhook_url:
                logger.error("Max: webhook transport requires MAX_WEBHOOK_URL")
                await self.disconnect()
                return False
            if not self._webhook_url.startswith("https://"):
                logger.error("Max: MAX_WEBHOOK_URL must start with https://")
                await self.disconnect()
                return False
            await self._start_webhook_server()
            if not await self._subscribe_webhook():
                await self.disconnect()
                return False
        else:
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
        for task in list(self._webhook_tasks):
            task.cancel()
        if self._webhook_tasks:
            await asyncio.gather(*self._webhook_tasks, return_exceptions=True)
            self._webhook_tasks.clear()
        if self._webhook_runner:
            try:
                await self._webhook_runner.cleanup()
            except Exception:
                logger.exception("Max webhook server cleanup failed")
            self._webhook_runner = None
        if self._session:
            await self._session.close()
            self._session = None
        self._mark_disconnected()

    async def _start_webhook_server(self) -> None:
        app = web.Application(client_max_size=MAX_WEBHOOK_MAX_BODY_BYTES)
        app.router.add_get(self._webhook_path, self._handle_webhook_health)
        app.router.add_post(self._webhook_path, self._handle_webhook_request)
        self._webhook_runner = web.AppRunner(app)
        await self._webhook_runner.setup()
        site = web.TCPSite(self._webhook_runner, self._webhook_host, self._webhook_port)
        await site.start()
        logger.info(
            "Max webhook listening on %s:%d%s for %s",
            self._webhook_host,
            self._webhook_port,
            self._webhook_path,
            self._webhook_url,
        )
        if not self._webhook_secret:
            logger.warning("Max webhook secret is not set; MAX recommends X-Max-Bot-Api-Secret validation")

    async def _subscribe_webhook(self) -> bool:
        body: dict[str, Any] = {
            "url": self._webhook_url,
            "update_types": self._webhook_update_types,
        }
        if self._webhook_secret:
            body["secret"] = self._webhook_secret

        result = await self._api_post("/subscriptions", json_body=body)
        if result.get("success") is True:
            logger.info("Max webhook subscribed: url=%s types=%s", self._webhook_url, self._webhook_update_types)
            return True
        if result:
            logger.error("Max webhook subscription failed: %s", _json_preview(result))
        return False

    async def _handle_webhook_health(self, request: "web.Request") -> "web.Response":
        return web.json_response({"status": "ok", "platform": "max"})

    async def _handle_webhook_request(self, request: "web.Request") -> "web.Response":
        if self._webhook_secret:
            provided = request.headers.get("X-Max-Bot-Api-Secret", "")
            if not hmac.compare_digest(provided, self._webhook_secret):
                logger.warning("Max webhook rejected: invalid X-Max-Bot-Api-Secret")
                return web.Response(status=401)

        try:
            raw = await request.read()
        except Exception:
            return web.Response(status=400)
        if len(raw) > MAX_WEBHOOK_MAX_BODY_BYTES:
            return web.Response(status=413)

        try:
            payload = json.loads(raw)
        except Exception:
            logger.warning("Max webhook rejected: invalid JSON body")
            return web.Response(status=400)
        if not isinstance(payload, dict):
            return web.Response(status=400)

        task = asyncio.create_task(self._dispatch_webhook_payload(payload))
        self._webhook_tasks.add(task)
        task.add_done_callback(self._webhook_tasks.discard)
        return web.Response(status=200)

    async def _dispatch_webhook_payload(self, payload: dict) -> None:
        try:
            updates = payload.get("updates")
            if isinstance(updates, list):
                for update in updates:
                    if isinstance(update, dict):
                        await self._handle_update(update)
                return
            await self._handle_update(payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Max webhook dispatch failed: %s", _json_preview(payload, limit=1200))

    # ── message sending ─────────────────────────────────────────

    async def _api_post(self, path: str, params: dict = None, json_body: dict = None) -> dict:
        """Helper: POST to Max API, return parsed JSON or {}."""
        if not self._session:
            return {}
        try:
            async with self._session.post(f"{self._api_base_url}{path}", params=params, json=json_body) as resp:
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

    async def _api_post_message(self, params: dict, body: Dict[str, Any]) -> dict:
        """POST /messages and retry as plain text if MAX rejects markdown."""
        if not self._session:
            return {}
        try:
            async with self._session.post(
                f"{self._api_base_url}/messages", params=params, json=body
            ) as resp:
                text = await resp.text()
                if resp.status == 200:
                    return json.loads(text) if text.strip() else {}
                logger.warning(f"Max API POST /messages → {resp.status}: {text[:300]}")
                if resp.status not in (400, 422) or body.get("format") != "markdown":
                    return {}
        except Exception as e:
            logger.warning(f"Max API POST /messages error: {e}")
            return {}

        fallback = dict(body)
        fallback.pop("format", None)
        logger.info("Max: retrying message without markdown formatting")
        return await self._api_post("/messages", params=params, json_body=fallback)

    async def _answer_callback(
        self,
        callback_id: str,
        *,
        notification: str = "",
        message: Optional[dict[str, Any]] = None,
    ) -> bool:
        if not callback_id:
            return False

        body: dict[str, Any] = {}
        if notification:
            body["notification"] = _max_trim_callback_text(notification)
        if message is not None:
            body["message"] = message
        if not body:
            return False

        result = await self._api_post(
            "/answers",
            params={"callback_id": callback_id},
            json_body=body,
        )
        return bool(result)

    async def _ack_callback_selection(
        self,
        callback_id: str,
        message: dict[str, Any],
        selected_text: str,
    ) -> None:
        if not callback_id:
            logger.warning("Max callback ack skipped: missing callback_id")
            return

        selected = _max_trim_callback_text(selected_text, limit=180)
        original_text = _max_message_text(message).strip()
        if original_text:
            updated_text = f"{original_text}\n\nВы выбрали: {selected}"
        else:
            updated_text = f"Вы выбрали: {selected}"

        answer_message: dict[str, Any] = {
            "text": updated_text,
            "attachments": [],
        }
        ok = await self._answer_callback(
            callback_id,
            notification=f"Выбрано: {selected}",
            message=answer_message,
        )
        if not ok:
            logger.warning("Max callback ack failed (callback_id=%s)", callback_id)

    async def _api_patch(self, path: str, json_body: dict = None) -> dict:
        """Helper: PATCH to Max API, return parsed JSON or {}."""
        if not self._session:
            return {}
        try:
            async with self._session.patch(f"{self._api_base_url}{path}", json=json_body) as resp:
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

    async def _send_text_message(
        self,
        chat_id,
        text: str,
        *,
        reply_to=None,
        attachments: Optional[list[dict[str, Any]]] = None,
    ) -> SendResult:
        body: Dict[str, Any] = {"text": text, "format": "markdown"}
        if reply_to:
            body["reply_to"] = reply_to
        if attachments:
            body["attachments"] = attachments
            for attachment in attachments:
                if _max_attachment_type(attachment) != "inline_keyboard":
                    continue
                buttons = (attachment.get("payload") or {}).get("buttons")
                if isinstance(buttons, list):
                    logger.info(
                        "Max: sending inline keyboard rows=%s preview=%s",
                        [
                            len(row) if isinstance(row, list) else 1
                            for row in buttons
                        ],
                        _json_preview(buttons, limit=500),
                    )

        result = await self._api_post_message({"chat_id": chat_id}, body)
        if result:
            msg_id = str(result.get("message_id") or result.get("message", {}).get("body", {}).get("mid", ""))
            return SendResult(success=True, message_id=msg_id)
        return SendResult(success=False, error="API returned empty response")

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        if not self._session:
            return SendResult(success=False, error="No HTTP session (disconnected)")

        if not content:
            return SendResult(success=False, error="Empty content")

        content, directive_attachments = _max_extract_inline_keyboard_directives(str(content))
        attachments = _max_metadata_attachments(metadata)
        attachments.extend(directive_attachments)
        if not attachments:
            auto_keyboard = _max_auto_keyboard_from_numbered_options(content)
            if auto_keyboard:
                logger.info("Max: inferred inline keyboard from trailing numbered options")
                attachments.append(auto_keyboard)
        if not content:
            return SendResult(success=False, error="Empty content")

        chunks = _split_max_text(str(content))
        if len(chunks) > 1:
            logger.info(
                "Max: splitting outbound text into %d chunks (chars=%d, chunk_limit=%d)",
                len(chunks),
                len(str(content)),
                MAX_MESSAGE_TEXT_CHUNK_SIZE,
            )

        message_ids: list[str] = []
        for index, chunk in enumerate(chunks):
            result = await self._send_text_message(
                chat_id,
                chunk,
                reply_to=reply_to if index == 0 else None,
                attachments=attachments if index == 0 else None,
            )
            if not result.success:
                return SendResult(
                    success=False,
                    error=f"Max text chunk {index + 1}/{len(chunks)} failed: {result.error}",
                    message_id=message_ids[-1] if message_ids else "",
                    continuation_message_ids=tuple(message_ids[:-1]),
                )
            message_ids.append(result.message_id or "")
        return SendResult(
            success=True,
            message_id=message_ids[-1] if message_ids else "",
            continuation_message_ids=tuple(message_ids[:-1]),
        )

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list],
        clarify_id: str,
        session_key: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Render Hermes clarify prompts as native MAX inline buttons."""
        if not choices:
            try:
                from tools.clarify_gateway import mark_awaiting_text

                mark_awaiting_text(clarify_id)
            except Exception:
                pass
            return await self.send(chat_id, f"❓ {question}", metadata=metadata)

        lines = [f"❓ {question}", ""]
        for index, choice in enumerate(choices, start=1):
            lines.append(f"{index}. {choice}")

        choice_buttons = [
            {
                "text": str(index + 1),
                "payload": f"cl:{clarify_id}:{index}",
            }
            for index, choice in enumerate(choices)
        ]
        buttons = [
            choice_buttons[index : index + 2]
            for index in range(0, len(choice_buttons), 2)
        ]
        buttons.append([{"text": "Свой вариант", "payload": f"cl:{clarify_id}:other"}])

        outgoing_metadata = dict(metadata or {})
        outgoing_metadata["max_inline_keyboard"] = buttons

        self._clarify_state[clarify_id] = session_key
        result = await self.send(
            chat_id,
            "\n".join(lines),
            metadata=outgoing_metadata,
        )
        if not result.success:
            self._clarify_state.pop(clarify_id, None)
        return result

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
                f"{self._api_base_url}/uploads",
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
                    upload_text = text.strip()
                    if upload_text:
                        try:
                            upload_result = json.loads(upload_text)
                        except json.JSONDecodeError:
                            # MAX video/audio uploads can return a small XML-ish
                            # body such as ``<retval>1</retval>``. The usable
                            # attachment token for those types is returned by
                            # POST /uploads, not by the upload-host response.
                            upload_result = {}
                    else:
                        upload_result = {}

            if upload_type in {"video", "audio"} and upload_init.get("token"):
                payload = {"token": upload_init["token"]}
                logger.info("Max: uploaded %s → payload keys=%s", file_path, sorted(payload.keys()))
                return payload

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
        max_attempts: int = 6,
    ) -> SendResult:
        caption_chunks = _split_max_text(caption or "")
        first_caption = caption_chunks[0] if caption_chunks else ""
        body = {
            "text": first_caption,
            "format": "markdown" if first_caption else None,
            "attachments": [{"type": attachment_type, "payload": payload}],
        }
        if body["format"] is None:
            body.pop("format")

        # MAX can accept the upload and still reject immediate /messages with
        # attachment.not.ready while the media backend finishes processing the
        # returned token. Retry with a small backoff; this matches the public
        # upload flow where video/audio tokens are sent only after upload
        # completion, but processing can lag for larger media.
        delays = (1.0, 2.0, 3.0, 5.0, 8.0)
        for attempt in range(max_attempts):
            result = await self._api_post_message({"chat_id": chat_id}, body)
            if result:
                message_ids = [str(result.get("message_id", ""))]
                for index, chunk in enumerate(caption_chunks[1:], start=2):
                    text_result = await self._send_text_message(chat_id, chunk)
                    if not text_result.success:
                        return SendResult(
                            success=False,
                            error=f"Max caption chunk {index}/{len(caption_chunks)} failed: {text_result.error}",
                            message_id=message_ids[-1] if message_ids else "",
                            continuation_message_ids=tuple(message_ids[:-1]),
                        )
                    message_ids.append(text_result.message_id or "")
                return SendResult(
                    success=True,
                    message_id=message_ids[-1] if message_ids else "",
                    continuation_message_ids=tuple(message_ids[:-1]),
                )
            if attempt < max_attempts - 1:
                await asyncio.sleep(delays[min(attempt, len(delays) - 1)])
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

    async def send_voice(
        self,
        chat_id,
        path=None,
        caption=None,
        reply_to=None,
        metadata=None,
        **kwargs,
    ) -> SendResult:
        """Send audio/voice from a local path.

        Hermes' generic gateway routes audio-like media with ``audio_path=...``;
        older adapter callers used the positional ``path`` argument. Accept both
        so MAX media delivery works from every gateway path.
        """
        audio_path = path or kwargs.get("audio_path") or kwargs.get("voice_path") or kwargs.get("file_path")
        if not audio_path:
            return SendResult(success=False, error="audio_path is required")

        payload = await self._upload_file(audio_path, "voice")
        if not payload:
            return SendResult(success=False, error="Max voice upload failed")
        return await self._send_attachment_message(chat_id, "audio", payload, caption)

    async def send_video(
        self,
        chat_id,
        path=None,
        caption=None,
        reply_to=None,
        metadata=None,
        **kwargs,
    ) -> SendResult:
        """Send video from a local path.

        Hermes' generic gateway calls platform adapters with ``video_path=...``
        when it extracts a ``MEDIA:/...mp4`` response. Keep the positional
        ``path`` form for direct callers, but also accept the generic keyword.
        """
        video_path = path or kwargs.get("video_path") or kwargs.get("file_path")
        if not video_path:
            return SendResult(success=False, error="video_path is required")

        payload = await self._upload_file(video_path, "video")
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
            result = await self._api_post_message({"chat_id": chat_id}, body)
            if result:
                return SendResult(success=True, message_id=str(result.get("message_id", "")))

        # Plain text fallback
        return await self.send(chat_id, f"{text}\n{url}" if url else text)

    # ── chat info ───────────────────────────────────────────────

    async def get_chat_info(self, chat_id) -> dict:
        if not self._session:
            return {"name": str(chat_id), "type": "dm"}

        try:
            async with self._session.get(f"{self._api_base_url}/chats/{chat_id}") as resp:
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
                    f"{self._api_base_url}/updates", params=params, timeout=aiohttp.ClientTimeout(total=40)
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
            payload = _max_callback_payload(u, callback)
            callback_id = _max_callback_id(u, callback)
            msg = u.get("message", {}) or {}
            recipient = msg.get("recipient", {}) or {}
            chat_id = str(
                msg.get("chat_id")
                or recipient.get("chat_id")
                or (callback.get("chat_id") if isinstance(callback, dict) else "")
                or u.get("chat_id", "")
            )
            user = (callback.get("user", {}) if isinstance(callback, dict) else {}) or u.get("user", {}) or {}
            author_id = str(user.get("user_id", ""))
            author_name = user.get("first_name") or user.get("name") or f"user_{author_id}"

            if isinstance(payload, str) and payload.startswith("cl:"):
                parts = payload.split(":", 2)
                if len(parts) == 3:
                    _, clarify_id, choice = parts
                    session_key = self._clarify_state.get(clarify_id)
                    if choice == "other":
                        await self._ack_callback_selection(
                            callback_id,
                            msg,
                            "Свой вариант",
                        )
                        try:
                            from tools.clarify_gateway import mark_awaiting_text

                            mark_awaiting_text(clarify_id)
                        except Exception as exc:
                            logger.warning("Max clarify other mark failed: %s", exc)
                        await self.send(chat_id, "Напиши свой вариант текстом.")
                        return

                    resolved_text = choice
                    try:
                        from tools.clarify_gateway import _entries as _clarify_entries  # type: ignore

                        entry = _clarify_entries.get(clarify_id)
                        if entry and entry.choices:
                            resolved_text = str(entry.choices[int(choice)])
                    except Exception:
                        resolved_text = choice

                    await self._ack_callback_selection(
                        callback_id,
                        msg,
                        resolved_text,
                    )

                    try:
                        from tools.clarify_gateway import resolve_gateway_clarify

                        resolved = resolve_gateway_clarify(clarify_id, resolved_text)
                    except Exception as exc:
                        logger.warning("Max clarify resolve failed: %s", exc)
                        resolved = False

                    if resolved:
                        self._clarify_state.pop(clarify_id, None)
                        logger.info(
                            "Max clarify button resolved (id=%s, choice=%r, session=%s)",
                            clarify_id,
                            resolved_text,
                            session_key,
                        )
                        return
                    logger.warning("Max clarify button had no waiter (id=%s)", clarify_id)

            callback_text = payload or _max_callback_text(u, callback)
            if not callback_text:
                logger.warning("Max callback ignored: no payload/text in update %s", _json_preview(u, limit=1200))
                return
            selected_text = _max_callback_text(u, callback) or callback_text
            await self._ack_callback_selection(callback_id, msg, selected_text)

            source = self.build_source(
                chat_id=chat_id,
                user_id=author_id,
                user_name=author_name,
            )

            event = MessageEvent(
                message_id=str(u.get("update_id", u.get("event_id", ""))),
                text=callback_text,
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
        if self._bot_user_id and author_id == self._bot_user_id:
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
    if not token:
        return False

    transport = str(
        os.getenv("MAX_TRANSPORT")
        or os.getenv("MAX_MODE")
        or extra.get("transport")
        or extra.get("mode")
        or ""
    ).strip().lower()
    webhook_url = str(os.getenv("MAX_WEBHOOK_URL") or extra.get("webhook_url") or "").strip()
    if transport == "webhook" or webhook_url:
        return webhook_url.startswith("https://")
    return True


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
    transport = os.getenv("MAX_TRANSPORT", "").strip()
    webhook_url = os.getenv("MAX_WEBHOOK_URL", "").strip()
    if transport:
        data["transport"] = transport
    elif webhook_url:
        data["transport"] = "webhook"
    if webhook_url:
        data["webhook_url"] = webhook_url
    webhook_secret = os.getenv("MAX_WEBHOOK_SECRET", "").strip()
    if webhook_secret:
        data["webhook_secret"] = webhook_secret
    webhook_host = os.getenv("MAX_WEBHOOK_HOST", "").strip()
    if webhook_host:
        data["webhook_host"] = webhook_host
    webhook_port = os.getenv("MAX_WEBHOOK_PORT", "").strip()
    if webhook_port:
        try:
            data["webhook_port"] = int(webhook_port)
        except ValueError:
            data["webhook_port"] = webhook_port
    webhook_path = os.getenv("MAX_WEBHOOK_PATH", "").strip()
    if webhook_path:
        data["webhook_path"] = webhook_path
    api_base_url = os.getenv("MAX_API_BASE_URL", "").strip()
    if api_base_url:
        data["api_base_url"] = api_base_url
    update_types = os.getenv("MAX_UPDATE_TYPES", "").strip()
    if update_types:
        data["update_types"] = [item.strip() for item in update_types.split(",") if item.strip()]
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
            "Для inline-кнопок можно добавить в конец ответа скрытый HTML-комментарий "
            "`<!-- max_buttons: [[{\"text\":\"Текст\",\"payload\":\"callback\"}]] -->` "
            "или кнопку-ссылку `<!-- max_buttons: [[{\"text\":\"Открыть\",\"url\":\"https://example.com\"}]] -->`; "
            "этот комментарий будет удален перед отправкой и превратится в кнопки Max. "
            "Если в ответе есть варианты выбора, меню, квестовые действия или список 2-6 действий, "
            "обязательно добавь max_buttons с этими вариантами; не оставляй выбор только текстом. "
            "Используй inline-кнопки только когда пользователю нужно выбрать короткое действие "
            "или открыть ссылку; обычно 2-4 кнопки достаточно. Не добавляй кнопки к обычным "
            "информационным ответам. "
            "Не говори, что отправка файлов недоступна, если файл лежит локально и есть абсолютный путь. "
            "Лимит сообщения: 4000 символов. Общайся на русском."
        ),
    )
