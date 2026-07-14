import asyncio
import importlib.util
from pathlib import Path

from gateway.config import PlatformConfig


def _load_max_adapter_module():
    adapter_path = Path(__file__).resolve().parents[1] / "adapter.py"
    spec = importlib.util.spec_from_file_location("hermes_max_adapter_test", adapter_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_max_voice_attachments_are_cached_for_gateway_stt():
    module = _load_max_adapter_module()

    class DummyMaxAdapter(module.MaxAdapter):
        async def _download_inbound_media(
            self, *, url, attachment_type, message_id, index
        ):
            assert attachment_type == "voice"
            return f"/tmp/{message_id}_{index}.ogg", "audio/ogg"

    adapter = DummyMaxAdapter(PlatformConfig(enabled=True, token="token"))
    media_urls, media_types = asyncio.run(
        adapter._resolve_inbound_media(
            [
                {
                    "type": "voice",
                    "payload": {"url": "https://maxvd.example/voice?id=1"},
                }
            ],
            "mid.123",
        )
    )

    assert media_urls == ["/tmp/mid.123_0.ogg"]
    assert media_types == ["audio/ogg"]


def test_max_audio_attachments_are_also_cached_for_stt():
    """MAX does not distinguish voice from audio; both need local STT files."""
    module = _load_max_adapter_module()

    class DummyMaxAdapter(module.MaxAdapter):
        async def _download_inbound_media(
            self, *, url, attachment_type, message_id, index
        ):
            assert attachment_type == "audio"
            return f"/tmp/{message_id}_{index}.ogg", "audio/ogg"

    adapter = DummyMaxAdapter(PlatformConfig(enabled=True, token="token"))
    media_urls, media_types = asyncio.run(
        adapter._resolve_inbound_media(
            [
                {
                    "type": "audio",
                    "payload": {"url": "https://maxvd.example/audio?id=1"},
                }
            ],
            "mid.123",
        )
    )

    assert media_urls == ["/tmp/mid.123_0.ogg"]
    assert media_types == ["audio/ogg"]


def test_max_audio_message_type_is_voice_for_stt():
    module = _load_max_adapter_module()

    assert module._max_message_type("", [{"type": "audio"}]) == module.MessageType.VOICE
    assert module._max_message_type("", [{"type": "voice"}]) == module.MessageType.VOICE


def test_max_voice_and_audio_unknown_content_type_use_supported_extension():
    module = _load_max_adapter_module()

    for attachment_type in ("voice", "audio"):
        assert module.MaxAdapter._extension_for_content_type("", attachment_type) == ".ogg"
        assert (
            module.MaxAdapter._extension_for_content_type(
                "application/octet-stream", attachment_type
            )
            == ".ogg"
        )


def test_plugin_registers_with_current_hermes_platform_api():
    module = _load_max_adapter_module()
    captured = {}

    class DummyContext:
        def register_platform(self, **kwargs):
            captured.update(kwargs)

    module.register(DummyContext())

    assert captured["name"] == "max"
    assert captured["adapter_factory"]
    assert captured["max_message_length"] == 4000
