import asyncio
from unittest import mock

from aioflow.helpers import try_call, load_config

__author__ = "a.lemets"


def test_try_call_not_callable():
    result = asyncio.run(try_call([], *[], **{}))
    assert result is None


def test_try_call_sync_function():
    def sync_func(var1, var2=None):
        assert var1 == 42
        assert var2 == "yoyo"
        return "enot"

    result = asyncio.run(try_call(sync_func, 42, var2="yoyo"))
    assert result == "enot"


def test_try_call_async_function():
    async def async_func(var1, var2=None):
        assert var1 == 42
        assert var2 == "yoyo"
        return "enot"

    result = asyncio.run(try_call(async_func, 42, var2="yoyo"))
    assert result == "enot"


def test_load_yaml_file():
    yaml = """
    celeryservice:
        timeout: 42
    """
    m = mock.mock_open(read_data=yaml)
    with mock.patch("aioflow.helpers.open", m):
        config = load_config("fake_path")

    assert config["celeryservice"]["timeout"] == 42
