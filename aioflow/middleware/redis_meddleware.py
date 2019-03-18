import datetime
from uuid import uuid4

from aioredis import Redis

from aioflow import MiddlewareABC, ServiceStatus
from aioflow.pipeline import PipelineStatus

__author__ = "a.lemets"


class RedisMiddleware(MiddlewareABC):
    def __init__(self, redis: Redis):
        self.redis = redis

    def _pipeline_key(self, pipeline_id):
        return f"pipeline:{pipeline_id}"

    def _service_key(self, service_id):
        return f"service:{service_id}"

    async def gen_id(self, key_function):
        _id = str(uuid4())
        while await self.redis.exists(key_function(_id)):
            _id = str(uuid4())
        return _id

    async def _pipeline_id(self):
        return await self.gen_id(self._pipeline_key)

    async def _service_id(self):
        return await self.gen_id(self._service_key)

    async def pipeline_create(self, pipeline, **kwargs):
        pipeline._id = await self._pipeline_id()
        await self.redis.hmset_dict(
            self._pipeline_key(pipeline.id),
            dict(
                name=pipeline.name,
                created=datetime.datetime.utcnow().timestamp(),
                status=PipelineStatus.PENDING.value,
            )
        )

    async def pipeline_start(self, pipeline, **kwargs):
        await self.redis.hmset_dict(
            self._pipeline_key(pipeline.id),
            dict(
                start=datetime.datetime.utcnow().timestamp(),
                status=PipelineStatus.PROCESSING.value,
            )
        )

    async def pipeline_message(self, pipeline, **kwargs):
        ...

    async def pipeline_done(self, pipeline, **kwargs):
        await self.redis.hmset_dict(
            self._pipeline_key(pipeline.id),
            dict(
                end=datetime.datetime.utcnow().timestamp(),
                status=PipelineStatus.DONE.value,
            )
        )

    async def pipeline_failed(self, pipeline, exception, **kwargs):
        await self.redis.hmset_dict(
            self._pipeline_key(pipeline.id),
            dict(
                end=datetime.datetime.utcnow().timestamp(),
                status=PipelineStatus.FAILED.value,
            )
        )

    async def service_create(self, service, **kwargs):
        service._id = await self._service_id()
        await self.redis.hmset_dict(
            self._service_key(service.id),
            dict(
                name=service.name,
                created=datetime.datetime.utcnow().timestamp(),
                status=ServiceStatus.PENDING.value,
            )
        )

    async def service_start(self, service, **kwargs):
        await self.redis.hmset_dict(
            self._service_key(service.id),
            dict(
                start=datetime.datetime.utcnow().timestamp(),
                status=ServiceStatus.PROCESSING.value,
            )
        )

    async def service_message(self, service, **kwargs):
        ...

    async def service_done(self, service, **kwargs):
        await self.redis.hmset_dict(
            self._service_key(service.id),
            dict(
                end=datetime.datetime.utcnow().timestamp(),
                status=ServiceStatus.DONE.value,
                result=service.json_result,
            )
        )

    async def service_failed(self, service, exception, **kwargs):
        await self.redis.hmset_dict(
            self._service_key(service.id),
            dict(
                end=datetime.datetime.utcnow().timestamp(),
                status=ServiceStatus.FAILED.value,
            )
        )
