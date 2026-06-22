# Hermes MAX Platform Plugin

MAX Messenger platform adapter for Hermes Agent.

This plugin lets Hermes receive and send messages through MAX (`max.ru`) using
either webhook delivery or long polling. It supports text, markdown formatting,
attachments, images, files, voice/audio, video, inline keyboards, callback
events, typing/read actions, and cron notification delivery to a default chat.

## Features

- Inbound messages through webhook or polling.
- Outbound text with automatic chunking for MAX message limits.
- Markdown-compatible formatting for bold, italic, strikethrough, code, links,
  and headings.
- Images, files, audio/voice, and video delivery.
- Inline keyboard attachments from send metadata.
- Callback event handling.
- Optional allow-list for permitted MAX user IDs.
- Optional default chat for cron and notification delivery.

## Requirements

- Hermes Agent with platform plugin support.
- Python package: `aiohttp`.
- A MAX bot token.

Install the runtime dependency in the same environment that runs Hermes:

```bash
pip install aiohttp
```

## Installation

Place this repository in the Hermes platform plugin directory as `max`:

```bash
mkdir -p ~/.hermes/plugins/platforms
git clone <your-repo-url> ~/.hermes/plugins/platforms/max
```

Then enable/configure the platform through your Hermes gateway configuration or
the Hermes plugin management UI, depending on how your Hermes deployment is
managed.

## Configuration

The plugin reads configuration from Hermes platform config and from environment
variables. Environment variables take precedence for deployment-specific values.

Required:

| Variable | Description |
| --- | --- |
| `MAX_BOT_TOKEN` | MAX bot token used as the API `Authorization` header. |

Optional:

| Variable | Description |
| --- | --- |
| `MAX_TRANSPORT` | `webhook` or `polling`. Defaults to `webhook` when `MAX_WEBHOOK_URL` is set, otherwise `polling`. |
| `MAX_WEBHOOK_URL` | Public HTTPS webhook URL registered with MAX, for example `https://example.com/max/webhook`. MAX expects external HTTPS on port 443. |
| `MAX_WEBHOOK_SECRET` | Secret validated against `X-Max-Bot-Api-Secret` on webhook requests. |
| `MAX_WEBHOOK_HOST` | Local webhook bind host. Defaults to `0.0.0.0`. |
| `MAX_WEBHOOK_PORT` | Local webhook listen port. Defaults to `8650`. |
| `MAX_WEBHOOK_PATH` | Local webhook path. Defaults to `/max/webhook`. |
| `MAX_UPDATE_TYPES` | Comma-separated update types. Defaults to `message_created,message_callback,bot_started`. |
| `MAX_API_BASE_URL` | MAX Bot API base URL. Defaults to `https://platform-api.max.ru`; override for tests, proxies, or compatible endpoints. |
| `MAX_ALLOWED_USERS` | Comma-separated MAX user IDs allowed to talk to the bot. |
| `MAX_ALLOW_ALL_USERS` | Set to `true` to allow any MAX user. Intended for development only. |
| `MAX_HOME_CHANNEL` | Default chat ID for cron and notification delivery. |
| `MAX_HOME_CHANNEL_NAME` | Optional display name for the home channel. |
| `MAX_HOME_CHANNEL_THREAD_ID` | Optional thread ID for home channel delivery metadata. |

## Webhook Mode

Webhook mode starts a local `aiohttp` server and registers the public webhook URL
with MAX.

Typical deployment shape:

1. Run Hermes with `MAX_TRANSPORT=webhook`.
2. Bind the plugin locally, for example `0.0.0.0:8650/max/webhook`.
3. Put a reverse proxy in front of it.
4. Expose the public URL over HTTPS on port 443.
5. Set `MAX_WEBHOOK_URL` to that public URL.

Example:

```bash
export MAX_BOT_TOKEN="..."
export MAX_TRANSPORT="webhook"
export MAX_WEBHOOK_URL="https://example.com/max/webhook"
export MAX_WEBHOOK_SECRET="change-me"
export MAX_WEBHOOK_HOST="0.0.0.0"
export MAX_WEBHOOK_PORT="8650"
export MAX_WEBHOOK_PATH="/max/webhook"
```

## Polling Mode

Polling mode does not require a public endpoint. It periodically reads updates
from the MAX API.

```bash
export MAX_BOT_TOKEN="..."
export MAX_TRANSPORT="polling"
```

If `MAX_TRANSPORT` is omitted and `MAX_WEBHOOK_URL` is not set, polling is used.

## Outbound Media

Hermes can send local files through platform adapters. When replying through
MAX, use an absolute local file path in a `MEDIA:` directive when Hermes needs
to deliver a generated file:

```text
MEDIA:/absolute/path/to/file.png
```

Audio files such as `.mp3`, `.wav`, `.m4a`, and `.ogg` are best delivered as
files/documents when preserving sound quality matters.

## Inline Keyboards

The adapter accepts MAX-specific metadata for inline keyboards. Supported
metadata shapes include:

```python
{
    "max_inline_keyboard": [
        [{"text": "Open", "url": "https://example.com"}],
        [{"text": "Pick", "payload": "resume:123"}],
    ]
}
```

or raw MAX attachments:

```python
{
    "max_attachments": [
        {"type": "inline_keyboard", "payload": {"buttons": [[...]]}}
    ]
}
```

## Development

Useful checks:

```bash
python -m py_compile adapter.py __init__.py
git status --short
```

This repository is maintained as an independent MAX platform plugin for Hermes.
The earliest implementation was imported from the public
`MaZzZilka/hermes-max-plugin` repository and then substantially reworked for
webhook support, media delivery, callbacks, chunking, and Hermes gateway
integration.
