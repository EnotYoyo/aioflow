import asyncio
from unittest import mock
from uuid import UUID

import pytest

from aioflow import Service, ServiceStatus
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
    assert pipeline._pre_callback is None
    assert pipeline._post_callback is None
    assert pipeline._on_message is None


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
    pipeline.register(ServiceForTests)
    with pytest.raises(AioFlowRuntimeError):
        pipeline.register(ServiceForTests, depends_on={"ServiceForTests": "sha1"})


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
    pipeline.register(ServiceForTests)
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
            return {"a": 23}

    class Service2(Service):
        async def payload(self, **kwargs):
            assert kwargs["service1.a"] == 23
            return {"b": 42}

    class Service3(Service):
        async def payload(self, **kwargs):
            assert kwargs["service1.a"] == 23
            return {"c": 11}

    class Service4(Service):
        async def payload(self, **kwargs):
            assert kwargs["service1.a"] == 23
            assert kwargs["service2.b"] == 42
            assert kwargs["service3.c"] == 11

    pipeline = Pipeline("test", config=config)
    pipeline.register(Service1)
    pipeline.register(Service2, depends_on={Service1: "a"})
    pipeline.register(Service3, depends_on={Service1: "a"})
    pipeline.register(Service4, depends_on={Service1: "a", Service2: "b", Service3: "c"})

    await pipeline.run()


@pytest.mark.asyncio
async def test_pipeline_pre_and_post_callbacks():
    config = {
        "__pre_callback_kwargs": {
            "a": 4,
            "b": 8
        },
        "__post_callback_kwargs": {
            "c": 23,
            "d": 42
        }
    }

    async def pre_call(a, b):
        assert a == 4
        assert b == 8
        pre_call.is_call = True

    async def post_call(c, d):
        assert c == 23
        assert d == 42
        post_call.is_call = True

    pipeline = Pipeline("test", config=config, pre_callback=pre_call, post_callback=post_call)
    await pipeline.run()

    assert pre_call.is_call
    assert post_call.is_call


@pytest.mark.asyncio
async def test_pipeline_bad_service_order():
    class Service1(ServiceForTests):
        ...

    class Service2(ServiceForTests):
        ...

    pipeline = Pipeline("test")
    with pytest.raises(AioFlowRuntimeError):
        pipeline.register(Service2, depends_on={Service1: "a"})

    pipeline.register(Service1)
    pipeline.register(Service2, depends_on={Service1: "a"})  # this its ok
