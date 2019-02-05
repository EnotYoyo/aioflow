import asyncio

__author__ = "a.lemets"


async def try_call(function, *args, **kwargs):
    result = None
    if callable(function):
        if asyncio.iscoroutinefunction(function):
            result = await function(*args, **kwargs)
        else:
            result = function(*args, **kwargs)
    return result
