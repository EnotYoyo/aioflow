import asyncio
import logging
from typing import Mapping

import yaml

__author__ = "a.lemets"

logger = logging.getLogger(__name__)


async def try_call(function, *args, **kwargs):
    result = None
    if callable(function):
        if asyncio.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
        else:
            result = function(*args, **kwargs)
    return result


def load_config(config_path):
    with open(config_path, 'r') as stream:
        try:
            return yaml.load(stream)
        except yaml.YAMLError as exc:
            logger.exception(f"Cant parse yaml config {config_path}")
            raise


def merge_dict(target_dict, dct):
    for key, value in dct.items():
        if key in target_dict and isinstance(target_dict[key], dict) and isinstance(dct[key], Mapping):
            merge_dict(target_dict[key], dct[key])
        else:
            target_dict[key] = value


class cached_property:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        result = instance.__dict__[self.func.__name__] = self.func(instance)
        return result
