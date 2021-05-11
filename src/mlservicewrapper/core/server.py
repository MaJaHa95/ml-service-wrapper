import inspect
import logging
import os
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

    async def _as_coroutine(self, val):
        
        if inspect.iscoroutine(val):
            return await val
        else:
            return val

    def has_load(self):
        return hasattr(self._service, "load")

    async def load(self, ctx: contexts.ServiceContext = None):
        load = getattr(self._service, 'load', None)

        if load:
           await self._as_coroutine(load(ctx))

        self._is_loaded = True

    async def process(self, ctx: contexts.ProcessContext):
        if not self._is_loaded and self.has_load():
            raise ValueError("Be sure to call load before process!")
        
        await self._as_coroutine(self._service.process(ctx))

    def dispose(self):
        disp = getattr(self._service, 'dispose', None)

        if disp:
            disp()
    
class ServerInstance:
    def __init__(self, host_type: str, config_path: str = None):

        if not config_path:
            config_path = os.environ.get("SERVICE_CONFIG_PATH", "./service/config.json")

        self.__config = configuration.ServiceConfiguration(config_path, host_type)

        self.__service_instance_loader = service_loading.get_service_loader(self.__config.service())

        self.__plugins = list(self.__config.plugins())

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

    def get_parameter_real_path_value(self, name: str) -> os.PathLike:
        return self.__config.parameters().get_real_path_value(name)
    
    def __get_service_instance(self) -> SafeServiceWrapper:

        service = self.__service_instance_loader.get_instance()

        print("Got service: {}".format(service.__class__.__name__))

        service = SafeServiceWrapper(service)

        for plugin in self.__plugins:
            plugin_loader = service_loading.get_service_loader(plugin, [service])
            
            print(f"loading plugin {plugin_loader.get_name()}")

            service = SafeServiceWrapper(plugin_loader.get_instance())

        return service

    def is_loaded(self):
        return self.__service is not None

    def _build_load_context_source(self, include_environment_variables = True, override: dict = None):
        context_parts: typing.List[context_sources.ServiceContextSource] = []
        
        if override is not None:
            context_parts.append(context_sources.DictServiceContextSource(override))

        if include_environment_variables:
            context_parts.append(context_sources.EnvironmentVariableServiceContextSource("SERVICE_"))
        
        context_parts.append(_ServiceConfigurationServiceContextSource(self.__config))

        return context_sources.CoalescingServiceContextSource(context_parts)

    def build_load_context(self, ctx_source: context_sources.ServiceContextSource = None):
        if ctx_source is None:
            ctx_source = self._build_load_context_source()

        ctx = contexts.ServiceContext(ctx_source, self._get_load_logger())

        return ctx

    def _get_load_logger(self):
        return self.__logger.getChild("load")
        
    def _get_process_logger(self):
        return self.__logger.getChild("process")

    async def load(self, ctx_source: context_sources.ServiceContextSource = None):
        service = self.__get_service_instance()

        if service.has_load():
            ctx = self.build_load_context(ctx_source)

            print("service.load")
            await service.load(ctx)

        self.__service = service

    async def process(self, ctx_source: context_sources.ProcessContextSource):
        logger = self._get_process_logger()
        ctx = contexts.ProcessContext(ctx_source, logger)
        
        await self.__service.process(ctx)

    def dispose(self):
        if self.__service is None:
            return
        
        self.__service.dispose()
