import asyncio
import logging

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
