import asyncio

from aioflow.helpers import try_call

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
