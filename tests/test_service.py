import pytest

from aioflow.service import Service, ServiceStatus, AioFlowBadStatus

__author__ = "a.lemets"


class PipelineMock:
    def register_service(self, *args, **kwargs):
        ...


class ServiceMock(Service):
    async def payload(self, **kwargs):
        return kwargs


class ServiceRaiseMock(Service):
    async def payload(self, **kwargs):
        raise ZeroDivisionError


@pytest.mark.asyncio
async def test_service_create_default():
    service = ServiceMock(PipelineMock())

    assert not service.allow_failure
    assert service.timeout is None
    assert service.name == "servicemock"
    assert repr(service) == "servicemock"
    assert service.id == f"ServiceMock__{id(service)}"
    assert service.status is ServiceStatus.PENDING
    assert not service.is_finished
    with pytest.raises(AioFlowBadStatus):
        assert service.result


@pytest.mark.asyncio
async def test_service_set_result():
    service = ServiceMock(PipelineMock())
    service.result = {"23": 42}

    assert service.status is ServiceStatus.DONE
    assert service.result == {"23": 42}


@pytest.mark.asyncio
async def test_service_return_result():
    service = ServiceMock(PipelineMock())
    await service(**{"this": "is", "awesome": "result"})

    assert service.is_finished
    assert service.status == ServiceStatus.DONE
    assert service.result == {"this": "is", "awesome": "result"}


@pytest.mark.asyncio
async def test_service_raise_exception():
    service = ServiceRaiseMock(PipelineMock())
    with pytest.raises(ZeroDivisionError):
        await service()

    assert service.is_finished
    assert service.status == ServiceStatus.FAILED
    assert service.result is None


@pytest.mark.asyncio
async def test_service_raise_exception_with_allow_failure():
    service = ServiceRaiseMock(PipelineMock(), allow_failure=True)
    await service()

    assert service.is_finished
    assert service.status == ServiceStatus.FAILED
    assert service.result is None
