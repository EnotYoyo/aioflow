import asyncio
import logging
from itertools import count
from typing import Dict, Callable, List, Iterator
from uuid import uuid4

from aioflow.helpers import try_call
from aioflow.service import Service

__author__ = 'a.lemets'

logger = logging.getLogger(__name__)


class AioFlowRuntimeError(RuntimeError):
    ...


class AioFlowKeyError(KeyError):
    ...


class Pipeline:
    def __init__(self,
                 name: str,
                 *,
                 config: Dict = None,
                 callback: Callable = None,
                 on_message: Callable = None):
        """

        :param name: name of pipeline
        :param config: path or config object
        :param callback: call, when pipeline is ended
        :param on_message: callback for messages (call from service)
        """
        self.name = name
        self.config = config
        self._uuid = str(uuid4())
        self._callback = callback
        self._on_message = on_message
        self._services = {}
        self._results_of = {}

    @property
    def id(self):
        return self._uuid

    @property
    def config(self) -> Dict:
        return self._config

    @config.setter
    def config(self, value: Dict or None):
        """
        Set config file
        :param value:
        :return:
        """
        if value is None:
            self._config = {}
        elif isinstance(value, Dict):
            self._config = dict(value)
        # elif isinstance(value, str):
        #     self._config = load_config(value)
        else:
            raise AioFlowRuntimeError("Bad value for config")

    async def message(self, **kwargs) -> None:
        """
        Try call message callback

        :param kwargs:
        :return:
        """
        return await try_call(self._on_message, **kwargs)

    async def callback(self, **kwargs) -> None:
        """
        Try call pipeline callback

        :param kwargs:
        :return:
        """

        return await try_call(self._callback, **kwargs)

    def register_service(self, service: Service, result_of: Dict = None) -> None:
        """
        Register new service in pipeline

        :param service: object of service
        :param result_of: dependence from another servic
        Ex.:
        result_of = {
            service_obj1: "res.key1",
            service_obj2: ["key1", "res.key_another"]
        }

        kwargs for service: **{
            f"{service_obj1.name}.res.key1": value1,
            f"{service_obj2.name}.key1": value2,
            f"{service_obj2.name}.res.key_another": value3,
        }

        :return: None
        """
        logger.debug(f"Try register {service.name}")

        if service.id in self._services:
            raise AioFlowRuntimeError(f"Service {service.name} already register")

        self._services[service.id] = service

        # convert {service: str} into {service: [str]}
        if result_of:
            for _service in result_of:
                if isinstance(result_of[_service], str):
                    result_of[_service] = [result_of[_service]]
        logger.debug(f"Service {service.name} needs result {result_of}")
        self._results_of[service.id] = result_of or {}

    @property
    def services(self):
        return self._services.values()

    def ready_services(self) -> Iterator[List]:
        """
        Generator for getting services for running

        :yield: List of service
        """
        # todo: FIND CYCLES!
        dependencies = {}
        for service_id in self._results_of:
            dependencies[service_id] = {service.id for service in self._results_of[service_id]}

        service_number = count(start=1)
        already_done = set()
        while True:
            scheduled = set()
            scheduled_tasks = []
            for service_id in self._results_of:
                if service_id not in already_done and dependencies[service_id] <= already_done:
                    scheduled.add(service_id)

                    kwargs = self.build_service_kwargs(service_id, next(service_number))
                    service = self._services[service_id]
                    scheduled_tasks.append(service(**kwargs))

            if not scheduled_tasks:
                break

            yield scheduled_tasks
            already_done |= scheduled

    def build_service_kwargs(self, service_id: str, service_number: int) -> dict:
        logger.debug("Build service kwargs")
        kwargs = {"__service_number": service_number}
        for service in self._results_of[service_id]:
            keys = self._results_of[service_id][service]
            for key in keys:
                res = service.result

                try:
                    for k in key.split("."):
                        res = res[k]
                except KeyError:
                    logger.error(f"Key {key} not found in {service.name}")
                    raise AioFlowKeyError(f"{key} not found in result of {service.name}")
                kwargs[f"{service.name}.{key}"] = res

        return kwargs

    async def run(self):
        for services in self.ready_services():
            await asyncio.wait(services)
        await self.callback()
