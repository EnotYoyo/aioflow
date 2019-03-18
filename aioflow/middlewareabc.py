import abc

__author__ = "a.lemets"


class MiddlewareABC(abc.ABC):
    async def pipeline_create(self, pipeline, **kwargs):
        ...

    async def pipeline_start(self, pipeline, **kwargs):
        ...

    async def pipeline_message(self, pipeline, **kwargs):
        ...

    async def pipeline_done(self, pipeline, **kwargs):
        ...

    async def pipeline_failed(self, pipeline, exception, **kwargs):
        ...

    async def service_create(self, service, **kwargs):
        ...

    async def service_start(self, service, **kwargs):
        ...

    async def service_message(self, service, **kwargs):
        ...

    async def service_done(self, service, **kwargs):
        ...

    async def service_failed(self, service, exception, **kwargs):
        ...
