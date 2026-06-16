"""
Max Messenger platform adapter for Hermes Gateway.

Uses long-polling (GET /updates) for receiving messages and
REST API (POST /messages) for sending.

API docs: https://dev.max.ru/docs-api
Base URL: https://platform-api.max.ru
"""

import asyncio
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
                "Content-Type": "application/json",
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
        await self._send_chat_action(chat_id, "typing_on", debounce_seconds=3.0)

    async def mark_seen(self, chat_id) -> None:
        """Mark incoming MAX messages as seen/read."""
        await self._send_chat_action(chat_id, "mark_seen")

    async def _upload_file(self, file_path: str, file_type: str = "file") -> Optional[str]:
        """Upload a file to Max and return the file_id."""
        if not self._session:
            return None

        path = Path(file_path)
        if not path.exists():
            logger.warning(f"Max upload: file not found: {file_path}")
            return None

        content_type_map = {
            "image": "image/png",
            "voice": "audio/ogg",
            "video": "video/mp4",
            "file": "application/octet-stream",
        }
        content_type = content_type_map.get(file_type, "application/octet-stream")

        try:
            data = aiohttp.FormData()
            data.add_field("file", path.open("rb"), filename=path.name, content_type=content_type)

            headers = {"Authorization": self.token}
            async with self._session.post(
                f"{MAX_API}/upload", data=data, headers=headers
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    file_id = result.get("file_id")
                    if file_id:
                        logger.info(f"Max: uploaded {file_path} → file_id={file_id}")
                        return file_id
                else:
                    body = await resp.text()
                    logger.warning(f"Max upload → {resp.status}: {body[:200]}")
        except Exception as e:
            logger.warning(f"Max upload error: {e}")
        return None

    async def send_image(self, chat_id, image_url, caption=None) -> SendResult:
        return await self._send_media(chat_id, image_url, "image", caption)

    async def send_image_file(self, chat_id, path, caption=None) -> SendResult:
        """Send image from local file path."""
        file_id = await self._upload_file(path, "image")
        if not file_id:
            return SendResult(success=False)

        body = {
            "text": caption or "",
            "format": "markdown" if caption else None,
            "attachments": [{"type": "image", "payload": {"file_id": file_id}}],
        }
        if body["format"] is None:
            body.pop("format")
        result = await self._api_post("/messages", params={"chat_id": chat_id}, json_body=body)
        if result:
            return SendResult(success=True, message_id=str(result.get("message_id", "")))
        return SendResult(success=False)

    async def send_document(self, chat_id, path, caption=None) -> SendResult:
        file_id = await self._upload_file(path, "file")
        if not file_id:
            return SendResult(success=False)

        body = {
            "text": caption or "",
            "format": "markdown" if caption else None,
            "attachments": [{"type": "file", "payload": {"file_id": file_id}}],
        }
        if body["format"] is None:
            body.pop("format")
        result = await self._api_post("/messages", params={"chat_id": chat_id}, json_body=body)
        if result:
            return SendResult(success=True, message_id=str(result.get("message_id", "")))
        return SendResult(success=False)

    async def send_voice(self, chat_id, path) -> SendResult:
        file_id = await self._upload_file(path, "voice")
        if not file_id:
            return SendResult(success=False)

        body = {
            "attachments": [{"type": "voice", "payload": {"file_id": file_id}}],
        }
        result = await self._api_post("/messages", params={"chat_id": chat_id}, json_body=body)
        if result:
            return SendResult(success=True, message_id=str(result.get("message_id", "")))
        return SendResult(success=False)

    async def send_video(self, chat_id, path, caption=None) -> SendResult:
        file_id = await self._upload_file(path, "video")
        if not file_id:
            return SendResult(success=False)

        body = {
            "text": caption or "",
            "format": "markdown" if caption else None,
            "attachments": [{"type": "video", "payload": {"file_id": file_id}}],
        }
        if body["format"] is None:
            body.pop("format")
        result = await self._api_post("/messages", params={"chat_id": chat_id}, json_body=body)
        if result:
            return SendResult(success=True, message_id=str(result.get("message_id", "")))
        return SendResult(success=False)

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
                    
                    # DEBUG
                    logger.warning(f"Max poll: marker={marker} new_marker={body.get('marker')} updates_count={len(updates) if isinstance(updates,list) else '?'}")
                    if updates and isinstance(updates, list) and len(updates) > 0:
                        logger.warning(f"Max poll: first update sample: {json.dumps(updates[0], default=str)[:500]}")
                    
                    if not isinstance(updates, list):
                        logger.warning(f"Max poll: unexpected format: {type(updates)}")
                        continue

                    new_marker = body.get("marker")
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

        logger.warning(f"Max: parsed chat_id={chat_id} text='{text[:50]}'")

        if not chat_id or not text:
            logger.warning(f"Max: skipping — no chat_id or text")
            return

        message_id = str(body.get("mid") or msg.get("message_id") or u.get("update_id", ""))
        dedup_key = (message_id, update_type, text)
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

        attachments_raw = body.get("attachments") or []
        has_attachments = len(attachments_raw) > 0

        event = MessageEvent(
            message_id=message_id,
            text=text,
            source=source,
            message_type=MessageType.ATTACHMENT if has_attachments else MessageType.TEXT,
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
            "Можно отправлять изображения, файлы, голосовые сообщения. "
            "Лимит сообщения: 4000 символов. Общайся на русском."
        ),
    )
