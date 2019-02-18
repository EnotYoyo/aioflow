# Aioflow
[![Build Status](https://travis-ci.org/EnotYoyo/aioflow.svg?branch=master)](https://travis-ci.org/EnotYoyo/aioflow)

A very simple library for creating python stream processing with asyncio.

```python
import asyncio

from aioflow import Pipeline, Service


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
    def pprint(**kwargs):
        print(kwargs)

    sha1_pipeline = Pipeline("sha1", on_message=pprint)
    get_sha1 = GetSha1(sha1_pipeline)
    PrintSha1(sha1_pipeline, result_of={get_sha1: "arc_sha1"})
    PrintSha1v2(sha1_pipeline, result_of={get_sha1: "arc_sha1"})
    await sha1_pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())

```

Results:
```bash
{'status': 'Processing', 'message': 'first sleep', '__service_name': 'GetSha1'}
{'status': 'Success', 'message': 'second sleep', '__service_name': 'GetSha1'}
{'status': 'Processing', 'message': 'first sleep', '__service_name': 'PrintSha1v2'}
{'GetSha1.arc_sha1': '42'}
{'status': 'Processing', 'message': 'first sleep', '__service_name': 'PrintSha1'}
{'GetSha1.arc_sha1': '42'}
{'status': 'Success', 'message': 'second sleep', '__service_name': 'PrintSha1v2'}
{'status': 'Success', 'message': 'second sleep', '__service_name': 'PrintSha1'}
```