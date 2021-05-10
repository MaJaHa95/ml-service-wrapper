import importlib
import inspect
import json
import logging
import os
import sys
import typing
from pathlib import Path

from . import configuration, context_sources, contexts, errors, services
from .internal import service_loading as service_loading

__all__ = ["ServerInstance"]

class _ServiceConfigurationServiceContextSource(context_sources.ServiceContextSource):
    def __init__(self, config: configuration.ServiceConfiguration):
        self.__parameters = config.parameters()
    
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        context_sources.NameValidator.raise_if_invalid(name)

        val = self.__parameters.get_value(name) or default
        
        if required and val is None:
            raise errors.MissingParameterError(name)

        return val

    def get_parameter_real_path_value(self, name: str, required: bool = True) -> Path:
        context_sources.NameValidator.raise_if_invalid(name)

        val = self.__parameters.get_real_path_value(name)
        
        if required and val is None:
            raise errors.MissingParameterError(name)

        return val

class SafeServiceWrapper(services.Service):
    def __init__(self, service: services.Service) -> None:
        self._service = service
        self._is_loaded = False

    async def _try_call_maybe_coroutine(self, inst: typing.Any, func_name: str, *args, **kwds):
        val = getattr(inst, func_name, None)

        if not val:
            return False

        await self._call_maybe_coroutine(val, *args, **kwds)

    async def _call_maybe_coroutine(self, func: typing.Callable, *args, **kwds):
        res = func(*args, **kwds)

        if inspect.iscoroutine(res):
            return await res
        else:
            return res

    def has_load(self):
        return hasattr(self._service, "load")

    async def load(self, ctx: contexts.ServiceContext = None):
        self._try_call_maybe_coroutine(self._service, 'load', ctx)

        self._is_loaded = True

    async def process(self, ctx: contexts.ProcessContext):
        if not self._is_loaded and self.has_load():
            raise ValueError("Be sure to call load before process!")
        
        self._call_maybe_coroutine(self._service.process, ctx)

    def dispose(self):
        self._try_call_maybe_coroutine(self._service, 'dispose')
    
class ServerInstance:
    def __init__(self, host_type: str, config_path: str = None):

        self.__host_type = host_type

        if not config_path:
            config_path = os.environ.get("SERVICE_CONFIG_PATH", "./service/config.json")

        self.__config = configuration.ServiceConfiguration(config_path)

        self.__service_instance_loader = service_loading.get_service_loader(self.__config.service())
        
        self.__host_configs = self.__config.host()

        self.__schema: configuration.ServiceSchema = self.__config.schema()
        self.__info: configuration.ServiceInfo = self.__config.info()
        
        self.__service: SafeServiceWrapper or None = None

        self.__logger = logging.getLogger(self.__service_instance_loader.get_name())
        
    def get_info(self) -> configuration.ServiceInfo:
        return self.__info

    def get_load_parameter_specs(self) -> configuration.ParametersSchema:
        return self.__schema.parameters_load()
        
    def get_process_parameter_specs(self) -> configuration.ParametersSchema:
        return self.__schema.parameters_process()
        
    def get_input_dataset_specs(self) -> typing.Iterable[configuration.DatasetSchema]:
        return self.__schema.datasets_input()

    def get_output_dataset_specs(self) -> typing.Iterable[configuration.DatasetSchema]:
        return self.__schema.datasets_output()

    def get_host_config_section(self) -> dict or None:
        if self.__host_configs is None:
            return None
        
        return self.__host_configs.get_host_config(self.__host_type)

    def get_parameter_real_path_value(self, name: str) -> os.PathLike:
        return self.__config.parameters().get_real_path_value(name)
    
    def __get_service_instance(self) -> SafeServiceWrapper:

        service = self.__service_instance_loader.get_instance()

        print("Got service: {}".format(service))

        service = SafeServiceWrapper(service)

        return service

    def is_loaded(self):
        return self.__service is not None

    def build_load_context_source(self, include_environment_variables = True, override: dict = None):
        context_parts: typing.List[context_sources.ServiceContextSource] = []
        
        if override is not None:
            context_parts.append(context_sources.DictServiceContextSource(override))

        if include_environment_variables:
            context_parts.append(context_sources.EnvironmentVariableServiceContextSource("SERVICE_"))
        
        if self.__config.has_parameters():
            context_parts.append(_ServiceConfigurationServiceContextSource(self.__config))

        return context_sources.CoalescingServiceContextSource(context_parts)

    def _get_load_logger(self):
        return self.__logger.getChild("load")
        
    def _get_process_logger(self):
        return self.__logger.getChild("process")

    async def load(self, ctx_source: context_sources.ServiceContextSource = None):
        service = self.__get_service_instance()


        if service.has_load():
            if ctx_source is None:
                ctx_source = self.build_load_context_source()

            ctx = contexts.ServiceContext(ctx_source, self._get_load_logger())

            print("service.load")
            await service.load(ctx)

        self.__service = service

    async def process(self, ctx_source: context_sources.ProcessContextSource):
        ctx = contexts.ProcessContext(ctx_source)
        
        await self.__service.process(ctx)

    def dispose(self):
        if self.__service is None:
            return
        
        self.__service.dispose()
