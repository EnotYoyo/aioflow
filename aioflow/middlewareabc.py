import abc

import aioflow

__author__ = "a.lemets"


class MiddlewareABC(abc.ABC):
    hooks = None

    async def pipeline_create(self, pipeline: "aioflow.Pipeline", **kwargs):
        ...

    async def pipeline_start(self, pipeline: "aioflow.Pipeline", **kwargs):
        ...

    async def pipeline_message(self, pipeline: "aioflow.Pipeline", **kwargs):
        ...

    async def pipeline_done(self, pipeline: "aioflow.Pipeline", **kwargs):
        ...

    async def pipeline_failed(self, pipeline: "aioflow.Pipeline", exception: Exception, **kwargs):
        ...

    async def service_create(self, service: "aioflow.Service", **kwargs):
        ...

    async def service_start(self, service: "aioflow.Service", **kwargs):
        ...

    async def service_message(self, service: "aioflow.Service", **kwargs):
        ...

    async def service_done(self, service: "aioflow.Service", **kwargs):
        ...

    async def service_failed(self, service: "aioflow.Service", exception: Exception, **kwargs):
        ...
