# Aioflow
[![Build Status](https://travis-ci.org/EnotYoyo/aioflow.svg?branch=master)](https://travis-ci.org/EnotYoyo/aioflow)
[![codecov](https://codecov.io/gh/EnotYoyo/aioflow/branch/master/graph/badge.svg)](https://codecov.io/gh/EnotYoyo/aioflow)

A very simple library for creating python stream processing with asyncio.
```bash
pip install aioflow
```


```python
import asyncio

from aioflow import Pipeline, Service, Middleware


class GetSha1(Service):
    async def payload(self, **kwargs):
        await asyncio.sleep(1)
        await self.message(status="Processing", message="first sleep")
        await asyncio.sleep(1)
        await self.message(status="Success", message="second sleep")
        return dict(arc_sha1="42")


class PrintSha1(Service):
    async def payload(self, **kwargs):
        await asyncio.sleep(1)
        await self.message(status="Processing", message="first sleep")
        print(kwargs)
        await asyncio.sleep(1)
        await self.message(status="Success", message="second sleep")


class PrintSha1v2(Service):
    async def payload(self, **kwargs):
        await asyncio.sleep(1)
        await self.message(status="Processing", message="first sleep")
        print(kwargs)
        await asyncio.sleep(1)
        await self.message(status="Success", message="second sleep")


async def main():
    class MessageMiddleware(Middleware):
        async def service_message(self, service, **kwargs):
            print(kwargs)

    sha1_pipeline = await Pipeline.create("sha1", middleware=MessageMiddleware())
    await sha1_pipeline.register(GetSha1)
    await sha1_pipeline.register(PrintSha1, depends_on={GetSha1: "arc_sha1"})
    await sha1_pipeline.register(PrintSha1v2, depends_on={GetSha1: "arc_sha1"})
    await sha1_pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())

```

Results:
```bash
{'status': 'Processing', 'message': 'first sleep', '__service_name': 'getsha1'}
{'status': 'Success', 'message': 'second sleep', '__service_name': 'getsha1'}
{'status': 'Processing', 'message': 'first sleep', '__service_name': 'printsha1'}
{'getsha1.arc_sha1': '42'}
{'status': 'Processing', 'message': 'first sleep', '__service_name': 'printsha1v2'}
{'getsha1.arc_sha1': '42'}
{'status': 'Success', 'message': 'second sleep', '__service_name': 'printsha1'}
{'status': 'Success', 'message': 'second sleep', '__service_name': 'printsha1v2'}
```

## Or with decorator syntax

```python
import asyncio

from aioflow import service_deco, Pipeline, Middleware


@service_deco(bind=True)
async def get_sha1(self):
    await asyncio.sleep(1)
    await self.message(status="Processing", message="first sleep")
    await asyncio.sleep(1)
    await self.message(status="Success", message="second sleep")
    return dict(arc_sha1="42")


@service_deco(bind=True)
async def print_sha1(self, **kwargs):
    await asyncio.sleep(1)
    await self.message(status="Processing", message="first sleep")
    print(kwargs)
    await asyncio.sleep(1)
    await self.message(status="Success", message="second sleep")


@service_deco(bind=True)
async def print_sha1v2(self, **kwargs):
    await asyncio.sleep(1)
    await self.message(status="Processing", message="first sleep")
    print(kwargs)
    await asyncio.sleep(1)
    await self.message(status="Success", message="second sleep")


async def main():
    class MessageMiddleware(Middleware):
        async def service_message(self, service, **kwargs):
            print(kwargs)

    sha1_pipeline = await Pipeline.create("sha1", middleware=MessageMiddleware())
    await sha1_pipeline.register(get_sha1)
    await sha1_pipeline.register(print_sha1, depends_on={get_sha1: "arc_sha1"})
    await sha1_pipeline.register(print_sha1v2, depends_on={get_sha1: "arc_sha1"})
    await sha1_pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())

```