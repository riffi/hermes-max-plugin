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

    # User-installed platform names become valid Platform pseudo-members only
    # after PluginContext.register_platform() has populated the runtime registry.
    # Mirror that lifecycle before constructing an adapter directly in tests.
    from gateway.platform_registry import PlatformEntry, platform_registry

    if not platform_registry.is_registered("max"):
        class TestPluginContext:
            def register_platform(self, **kwargs):
                kwargs.setdefault("source", "plugin")
                platform_registry.register(PlatformEntry(**kwargs))

        module.register(TestPluginContext())

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


def test_outbound_audio_defaults_to_document_delivery():
    module = _load_max_adapter_module()
    captured = {}

    class DummyMaxAdapter(module.MaxAdapter):
        async def send_document(self, **kwargs):
            captured.update(kwargs)
            return module.SendResult(success=True, message_id="document-1")

        async def _upload_file(self, path, attachment_type):
            raise AssertionError("native voice upload must not run by default")

    adapter = DummyMaxAdapter(PlatformConfig(enabled=True, token="token"))
    result = asyncio.run(
        adapter.send_voice(
            chat_id="chat-1",
            audio_path="/tmp/original.mp3",
            caption="Track",
        )
    )

    assert result.success is True
    assert result.message_id == "document-1"
    assert captured["file_path"] == "/tmp/original.mp3"
    assert captured["caption"] == "Track"


def test_native_voice_delivery_requires_explicit_opt_in():
    module = _load_max_adapter_module()
    uploaded = {}
    sent = {}

    class DummyMaxAdapter(module.MaxAdapter):
        async def _upload_file(self, path, attachment_type):
            uploaded.update(path=path, attachment_type=attachment_type)
            return {"token": "voice-token"}

        async def _send_attachment_message(
            self, chat_id, attachment_type, payload, caption=None
        ):
            sent.update(
                chat_id=chat_id,
                attachment_type=attachment_type,
                payload=payload,
                caption=caption,
            )
            return module.SendResult(success=True, message_id="voice-1")

    adapter = DummyMaxAdapter(PlatformConfig(enabled=True, token="token"))
    result = asyncio.run(
        adapter.send_voice(
            chat_id="chat-1",
            audio_path="/tmp/voice.ogg",
            metadata={"max_native_voice": True},
        )
    )

    assert result.success is True
    assert uploaded == {"path": "/tmp/voice.ogg", "attachment_type": "voice"}
    assert sent["attachment_type"] == "audio"


def test_connect_accepts_gateway_reconnect_keyword():
    module = _load_max_adapter_module()
    adapter = module.MaxAdapter(PlatformConfig(enabled=True, token="token"))
    adapter.token = ""

    assert asyncio.run(adapter.connect(is_reconnect=True)) is False
