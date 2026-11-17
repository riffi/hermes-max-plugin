import asyncio

from gateway.config import PlatformConfig
from test_voice_media_cache import _load_max_adapter_module


def test_native_commands_use_current_max_endpoint():
    module = _load_max_adapter_module()
    calls = []

    class DummyMaxAdapter(module.MaxAdapter):
        async def _api_patch(self, path, json_body):
            calls.append((path, json_body))
            return {"commands": json_body["commands"]}

    adapter = DummyMaxAdapter(PlatformConfig(enabled=True, token="test-token"))
    asyncio.run(adapter._register_native_commands())

    assert calls == [("/me/commands", {"commands": module.MAX_NATIVE_COMMANDS})]
