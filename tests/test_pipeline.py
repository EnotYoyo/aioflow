import asyncio
from unittest import mock
from uuid import UUID

import pytest

from aioflow import Service, ServiceStatus
from aioflow.middlewareabc import MiddlewareABC
from aioflow.pipeline import Pipeline, AioFlowRuntimeError, AioFlowKeyError

__author__ = "a.lemets"


class ServiceForTests(Service):
    async def payload(self, **kwargs):
        return kwargs


class ServiceRaise(Service):
    async def payload(self, **kwargs):
        raise ZeroDivisionError


class ServiceWithTimeout(Service):
    async def payload(self, **kwargs):
        await asyncio.sleep(1)


def test_pipeline_create():
    pipeline = Pipeline("test_name", config={"this": "is", "awesome": "config"})

    assert pipeline.name == "test_name"
    assert pipeline.config == {"this": "is", "awesome": "config"}
    assert str(UUID(pipeline.id)) == pipeline.id


@pytest.mark.asyncio
async def test_register_service():
    pipeline = Pipeline("test_name")
    service = ServiceForTests(pipeline)
    pipeline._register_service(service)

    assert pipeline._services == {service.id: service}
    assert pipeline._depends_on == {service.id: {}}


@pytest.mark.asyncio
async def test_register_service_with_depends_on():
    pipeline = Pipeline("test_name")
    first_service = ServiceForTests(pipeline)
    pipeline._register_service(first_service)
    second_service = ServiceForTests(pipeline)
    pipeline._register_service(second_service, depends_on={ServiceForTests: "sha1"})

    assert pipeline._services == {first_service.id: first_service, second_service.id: second_service}
    assert pipeline._depends_on == {first_service.id: {}, second_service.id: {first_service: ["sha1"]}}
    assert list(pipeline.services) == [first_service, second_service]


@pytest.mark.asyncio
async def test_register_service_with_bad_depends_on():
    pipeline = Pipeline("test_name")
    await pipeline.register(ServiceForTests)
    with pytest.raises(AioFlowRuntimeError):
        await pipeline.register(ServiceForTests, depends_on={"ServiceForTests": "sha1"})


def test_pipeline_dict_config():
    pipeline = Pipeline("test", config={"a": 23, "b": 42})
    assert pipeline.config == {"a": 23, "b": 42}


def test_pipeline_yaml_config():
    yaml = """
        a:
            23
        b:
            42
        """
    m = mock.mock_open(read_data=yaml)
    with mock.patch("aioflow.helpers.open", m):
        pipeline = Pipeline("test", config="yaml_path")
    assert pipeline.config == {"a": 23, "b": 42}


def test_pipeline_update_config():
    pipeline = Pipeline("test", config={"a": {"c": 23}, "b": 42, "c": 1})
    pipeline.update_config({
        "a": {"c": 11, "d": {"a": 1}},
        "b": {"d": 23}
    })

    assert pipeline.config == {"a": {"c": 11, "d": {"a": 1}}, "b": {"d": 23}, "c": 1}


@pytest.mark.asyncio
async def test_pipeline_build_service_kwargs_without_dependence():
    config = {
        "servicefortests": {
            "__kwargs": {
                "a": 23,
                "b": 42
            }
        }
    }
    pipeline = Pipeline("test", config=config)
    await pipeline.register(ServiceForTests)
    kwargs = pipeline.build_service_kwargs(list(pipeline.services)[0], 1)
    assert kwargs == {"a": 23, "b": 42, "__service_number": 1}


@pytest.mark.asyncio
async def test_pipeline_build_service_kwargs_with_dependence():
    class ServiceForTests1(ServiceForTests):
        ...

    class ServiceForTests2(ServiceForTests):
        ...

    config = {
        "servicefortests1": {
            "__kwargs": {
                "a": 23,
                "b": 42
            }
        }
    }
    pipeline = Pipeline("test", config=config)
    service = ServiceForTests(pipeline)
    service.result = {
        "a": 48,
        "b.c.d.e": "bad key",
        "e": "fake",
        "b": {
            "e": "fake",
            "c": {
                "e": "fake",
                "d": {
                    "e": 1516,
                    "j": 12
                }
            }}
    }
    pipeline._register_service(service)

    service1 = ServiceForTests1(pipeline)
    pipeline._register_service(service1, depends_on={ServiceForTests: ["a", "b.c.d.e"]})

    kwargs = pipeline.build_service_kwargs(service1, 1)
    assert kwargs == {"a": 23, "b": 42, "__service_number": 1, "servicefortests.a": 48, "servicefortests.b.c.d.e": 1516}

    service3 = ServiceForTests2(pipeline)
    pipeline._register_service(service3, depends_on={ServiceForTests: "bad_key"})
    with pytest.raises(AioFlowKeyError):
        pipeline.build_service_kwargs(service3, 2)


@pytest.mark.asyncio
async def test_service_return_result():
    config = {
        "servicefortests": {
            "__kwargs": {
                "this": "is",
                "awesome": "result"
            }
        }
    }
    pipeline = Pipeline("test", config=config)
    service = ServiceForTests(pipeline)
    pipeline._register_service(service)
    await pipeline.run()

    assert service.is_finished
    assert service.status == ServiceStatus.DONE
    assert service.result == {"this": "is", "awesome": "result"}


@pytest.mark.asyncio
async def test_service_raise_exception():
    pipeline = Pipeline("test")
    service = ServiceRaise(pipeline)
    pipeline._register_service(service)

    with pytest.raises(ZeroDivisionError):
        await pipeline.run()

    assert service.is_finished
    assert service.status == ServiceStatus.FAILED
    assert service.result is None


@pytest.mark.asyncio
async def test_service_raise_exception_with_allow_failure():
    pipeline = Pipeline("test", config={"serviceraise": {"allow_failure": True}})
    service = ServiceRaise(pipeline)
    pipeline._register_service(service)
    await pipeline.run()

    assert service.is_finished
    assert service.status == ServiceStatus.FAILED
    assert service.result is None


@pytest.mark.asyncio
async def test_service_with_timeout():
    pipeline = Pipeline("test", config={"servicewithtimeout": {"timeout": 0.1}})
    service = ServiceWithTimeout(pipeline)
    pipeline._register_service(service)
    with pytest.raises(asyncio.TimeoutError):
        await pipeline.run()

    assert service.is_finished
    assert service.status == ServiceStatus.FAILED
    assert service.result is None


@pytest.mark.asyncio
async def test_pipeline_build_services_dependence():
    config = {
        "servicefortests": {
            "__kwargs": {
                "a": 23,
                "b": 42,
                "c": 11
            }
        }
    }

    class Service1(Service):
        async def payload(self, **kwargs):
            self.__class__.callable = True
            return {"a": 23}

    class Service2(Service):
        async def payload(self, **kwargs):
            self.__class__.callable = True
            assert kwargs["service1.a"] == 23
            return {"b": 42}

    class Service3(Service):
        async def payload(self, **kwargs):
            self.__class__.callable = True
            assert kwargs["service1.a"] == 23
            return {"c": 11}

    class Service4(Service):
        async def payload(self, **kwargs):
            self.__class__.callable = True
            assert kwargs["service1.a"] == 23
            assert kwargs["service2.b"] == 42
            assert kwargs["service3.c"] == 11

    pipeline = Pipeline("test", config=config)
    await pipeline.register(Service1)
    await pipeline.register(Service2, depends_on={Service1: "a"})
    await pipeline.register(Service3, depends_on={Service1: "a"})
    await pipeline.register(Service4, depends_on={Service1: "a", Service2: "b", Service3: "c"})

    await pipeline.run()
    assert Service1.callable
    assert Service2.callable
    assert Service3.callable
    assert Service4.callable


@pytest.mark.asyncio
async def test_pipeline_bad_service_order():
    class Service1(ServiceForTests):
        ...

    class Service2(ServiceForTests):
        ...

    pipeline = Pipeline("test")
    with pytest.raises(AioFlowRuntimeError):
        await pipeline.register(Service2, depends_on={Service1: "a"})

    await pipeline.register(Service1)
    await pipeline.register(Service2, depends_on={Service1: "a"})  # this its ok


@pytest.mark.asyncio
async def test_pipeline_middleware_create_start_end_message():
    class TestMiddleware(MiddlewareABC):
        async def pipeline_create(self, pipeline, **kwargs) -> id:
            self.__class__._pipeline_create = True
            assert kwargs == {"a": 4, "b": 8}

        async def pipeline_start(self, pipeline, **kwargs):
            self.__class__._pipeline_start = True
            assert kwargs == {"c": 15, "d": 16}

        async def pipeline_message(self, pipeline, **kwargs):
            self.__class__._pipeline_message = True
            assert kwargs == {"yo": "yo"}

        async def pipeline_done(self, pipeline, **kwargs):
            self.__class__._pipeline_done = True
            assert kwargs == {"e": 23, "f": 42}

    config = {
        "__pipeline_create_kwargs": {
            "a": 4,
            "b": 8
        },
        "__pipeline_start_kwargs": {
            "c": 15,
            "d": 16
        },
        "__pipeline_done_kwargs": {
            "e": 23,
            "f": 42
        }
    }

    pipeline = await Pipeline.create("test", config=config, middleware=TestMiddleware())
    await pipeline.run()
    await pipeline.message(**{"yo": "yo"})

    assert TestMiddleware._pipeline_create
    assert TestMiddleware._pipeline_start
    assert TestMiddleware._pipeline_message
    assert TestMiddleware._pipeline_done


@pytest.mark.asyncio
async def test_pipeline_middleware_failed():
    class TestMiddleware(MiddlewareABC):
        async def pipeline_failed(self, pipeline, exception, **kwargs) -> id:
            self.__class__._pipeline_failed = True
            assert isinstance(exception, ZeroDivisionError)

    pipeline = await Pipeline.create("test", middleware=TestMiddleware())
    await pipeline.register(ServiceRaise)

    with pytest.raises(ZeroDivisionError):
        await pipeline.run()

    assert TestMiddleware._pipeline_failed


@pytest.mark.asyncio
async def test_pipeline_middleware_failed():
    class TestMiddleware(MiddlewareABC):
        async def pipeline_failed(self, pipeline, exception, **kwargs) -> id:
            self.__class__._pipeline_failed = True
            assert isinstance(exception, ZeroDivisionError)

    pipeline = await Pipeline.create("test", middleware=TestMiddleware())
    await pipeline.register(ServiceRaise)

    with pytest.raises(ZeroDivisionError):
        await pipeline.run()

    assert TestMiddleware._pipeline_failed


@pytest.mark.asyncio
async def test_pipeline_middleware_service():
    class TestMiddleware(MiddlewareABC):
        async def service_create(self, service, **kwargs):
            self.__class__._service_create = True
            assert isinstance(service, MessageService)

        async def service_start(self, service, **kwargs):
            self.__class__._service_start = True
            assert isinstance(service, MessageService)
            assert service.status is ServiceStatus.PROCESSING

        async def service_message(self, service, **kwargs):
            self.__class__._service_message = True
            assert kwargs == {"yo": "yo"}
            assert isinstance(service, MessageService)
            assert service.status is ServiceStatus.PROCESSING

        async def service_done(self, service, **kwargs):
            self.__class__._service_done = True
            assert isinstance(service, MessageService)
            assert service.status is ServiceStatus.DONE

    class MessageService(ServiceForTests):
        async def payload(self, **kwargs):
            await self.message(**{"yo": "yo"})
            return kwargs

    pipeline = await Pipeline.create("test", middleware=TestMiddleware())
    await pipeline.register(MessageService)

    assert TestMiddleware._service_create

    await pipeline.run()
    assert TestMiddleware._service_start
    assert TestMiddleware._service_message
    assert TestMiddleware._service_done
