import asyncio

import pytest

from aioflow.service import Service, ServiceStatus, AioFlowBadStatus, service_deco

__author__ = "a.lemets"


class PipelineMock:
    def __init__(self, config=None):
        self._config = config or {}

    @property
    def config(self):
        return self._config


class PipelineMockWithMessage(PipelineMock):
    def __init__(self, expected_message_args=None, expected_message_kwargs=None):
        super().__init__()
        self.expected_message_args = expected_message_args or ()
        self.expected_message_kwargs = expected_message_kwargs or {}

    async def message(self, *args, **kwargs):
        assert self.expected_message_args == args
        assert self.expected_message_kwargs == kwargs


class ServiceForTests(Service):
    async def payload(self, **kwargs):
        return kwargs


class ServiceForTestsWithMessage(Service):
    async def payload(self, **kwargs):
        await self.message(**kwargs)
        return kwargs


@pytest.mark.asyncio
async def test_service_create_default():
    service = ServiceForTests(PipelineMock())

    assert not service.allow_failure
    assert service.timeout is None
    assert service.name == "servicefortests"
    assert repr(service) == "servicefortests"
    assert service.id == f"ServiceForTests__{id(service)}"
    assert service.status is ServiceStatus.PENDING
    assert not service.is_finished
    assert service.config == {}
    with pytest.raises(AioFlowBadStatus):
        assert service.result


@pytest.mark.asyncio
async def test_service_create_with_config():
    config = {
        "servicefortests": {
            "timeout": 42,
            "allow_failure": True
        },
        "timeout": 23,
    }
    service = ServiceForTests(PipelineMock(config=config))

    assert service.allow_failure
    assert service.timeout == 42
    assert service.name == "servicefortests"
    assert repr(service) == "servicefortests"
    assert service.id == f"ServiceForTests__{id(service)}"
    assert service.status is ServiceStatus.PENDING
    assert not service.is_finished
    assert service.config == {"timeout": 42, "allow_failure": True}
    with pytest.raises(AioFlowBadStatus):
        assert service.result


@pytest.mark.asyncio
async def test_service_config():
    config = {
        "__global": {
            "timeout": 23,
            "some": "value"
        },
        "servicefortests": {
            "timeout": 42,
            "allow_failure": True
        },
    }
    service = ServiceForTests(PipelineMock(config=config))
    assert service.allow_failure
    assert service.timeout == 42
    assert service.config == {"timeout": 42, "allow_failure": True, "some": "value"}

    config = service.config
    assert id(config) == id(service.config)  # check cached_property


@pytest.mark.asyncio
async def test_service_set_result():
    service = ServiceForTests(PipelineMock())
    service.result = {"23": 42}

    assert service.status is ServiceStatus.DONE
    assert service.result == {"23": 42}


@pytest.mark.asyncio
async def test_service_run():
    service = ServiceForTests(PipelineMock())
    res = await service(a=23, b=42)

    assert res == {"a": 23, "b": 42}


@pytest.mark.asyncio
async def test_service_get_loop():
    service = ServiceForTests(PipelineMock())
    loop = asyncio.get_event_loop()

    assert service.loop == loop


@pytest.mark.asyncio
async def test_service_decorator():
    @service_deco
    async def test_payload(**kwargs):
        return kwargs

    assert issubclass(test_payload, Service)

    pipeline = PipelineMock()
    service = test_payload(pipeline)
    res = await service(a=23, b=42)

    assert res == {"a": 23, "b": 42}


@pytest.mark.asyncio
async def test_service_decorator_with_bind():
    @service_deco(bind=True)
    async def test_payload(self, **kwargs):
        assert isinstance(self, Service)
        return kwargs

    pipeline = PipelineMock()
    service = test_payload(pipeline)
    res = await service(a=23, b=42)

    assert res == {"a": 23, "b": 42}


@pytest.mark.asyncio
async def test_service_decorator_with_payload_name():
    class TestService(Service):
        async def payload(self, **kwargs):
            info = await self.get_info(**kwargs)
            assert info == kwargs
            return info

        async def get_info(self, **kwargs):
            ...

    @service_deco(base_service_cls=TestService, payload_name="get_info")
    async def test_service(**kwargs):
        return kwargs

    assert issubclass(test_service, TestService)

    pipeline = PipelineMock()
    service = test_service(pipeline)

    res = await service(a=23, b=42)

    assert res == {"a": 23, "b": 42}
