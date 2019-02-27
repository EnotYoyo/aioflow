import asyncio
from unittest import mock

import pytest
import yaml

from aioflow.helpers import try_call, load_config, merge_dict

__author__ = "a.lemets"


def test_try_call_not_callable():
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(try_call([], *[], **{}))
    assert result is None


def test_try_call_sync_function():
    def sync_func(var1, var2=None):
        assert var1 == 42
        assert var2 == "yoyo"
        return "enot"

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(try_call(sync_func, 42, var2="yoyo"))
    assert result == "enot"


def test_try_call_async_function():
    async def async_func(var1, var2=None):
        assert var1 == 42
        assert var2 == "yoyo"
        return "enot"

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(try_call(async_func, 42, var2="yoyo"))
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


def test_load_bad_yaml_file():
    bad_yaml = """{"celeryservice" = {"timeout" = 42}}"""
    m = mock.mock_open(read_data=bad_yaml)
    with mock.patch("aioflow.helpers.open", m):
        with pytest.raises(yaml.YAMLError):
            load_config("fake_path")


def test_merge_dict():
    dct1 = {"a": 1, "b": 2}
    dct2 = {"c": 3, "d": 4}
    merge_dict(dct1, dct2)

    assert dct1 == {"a": 1, "b": 2, "c": 3, "d": 4}
    assert dct2 == {"c": 3, "d": 4}


def test_merge_dict_with_conflict():
    dct1 = {"a": {"b": 1}, "c": {"d": 2}}
    dct2 = {"c": 3, "d": 4}
    merge_dict(dct1, dct2)

    assert dct1 == {"a": {"b": 1}, "c": 3, "d": 4}
    assert dct2 == {"c": 3, "d": 4}


def test_merge_dict_recursive():
    dct1 = {"a": {"b": 1}, "c": {"d": 2}}
    dct2 = {"a": {"c": 3}, "c": {"d": 4}}
    merge_dict(dct1, dct2)

    assert dct1 == {"a": {"b": 1, "c": 3}, "c": {"d": 4}}
    assert dct2 == {"a": {"c": 3}, "c": {"d": 4}}
