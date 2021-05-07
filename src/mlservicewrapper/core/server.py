import importlib

import inspect
import json
import os
import sys
import typing
from pathlib import Path

import mlservicewrapper.core.internal.service_loading as service_loading
import mlservicewrapper.core.configuration as configuration

from mlservicewrapper.core import contexts, errors, services

__all__ = ["ServerInstance"]

class _ConfigurationFileServiceContext(contexts.ServiceContext):
    def __init__(self, config: configuration.ServiceConfiguration):
        self.__parameters = config.parameters()
    
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        contexts.NameValidator.raise_if_invalid(name)

        val = self.__parameters.get_value(name) or default
        
        if required and val is None:
            raise errors.MissingParameterError(name)

        return val

    def get_parameter_real_path_value(self, name: str, required: bool = True) -> Path:
        contexts.NameValidator.raise_if_invalid(name)

        val = self.__parameters.get_real_path_value(name)
        
        if required and val is None:
            raise errors.MissingParameterError(name)

        return val


class ServerInstance:
    def __init__(self, config_path: str = None):

        if not config_path:
            config_path = os.environ.get("SERVICE_CONFIG_PATH", "./service/config.json")

        self.__config = configuration.ServiceConfiguration(config_path)

        self.__service_instance_loader = service_loading.get_service_loader(self.__config.service())
        
        self.__host_configs = self.__config.host()

        self.__schema: configuration.ServiceSchema = self.__config.schema()
        self.__info: configuration.ServiceInfo = self.__config.info()
        
        self.__service: services.Service = None

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

    def get_host_config_section(self, name: str) -> dict or None:
        if self.__host_configs is None:
            return None
        
        return self.__host_configs.get_host_config(name)

    def get_parameter_real_path_value(self, name: str) -> os.PathLike:
        return self.__config.parameters().get_real_path_value(name)
    
    def __get_service_instance(self) -> services.Service:

        service = self.__service_instance_loader.get_instance()

        print("Got service: {}".format(service))

        return service

    def is_loaded(self):
        return self.__service is not None

    def build_context(self, include_environment_variables = True, override: dict = None):
        context_parts: typing.List[contexts.ServiceContext] = []
        
        if override is not None:
            context_parts.append(contexts.DictServiceContext(override))

        if include_environment_variables:
            context_parts.append(contexts.EnvironmentVariableServiceContext("SERVICE_"))
        
        if self.__config.has_parameters():
            context_parts.append(_ConfigurationFileServiceContext(self.__config))

        return contexts.CoalescingServiceContext(context_parts)


    async def load(self, ctx: contexts.ServiceContext = None):
        service = self.__get_service_instance()
        
        if hasattr(service, 'load'):
            if ctx is None:
                ctx = self.build_context()

            print("service.load")
            load_result = service.load(ctx)

            if inspect.iscoroutine(load_result):
                await load_result

        self.__service = service

    async def process(self, ctx: contexts.ProcessContext):
        if not self.is_loaded():
            raise ValueError("Be sure to call load before process!")
        
        process_result = self.__service.process(ctx)

        if inspect.iscoroutine(process_result):
            await process_result

    def dispose(self):
        if self.__service is None or not hasattr(self.__service, 'dispose'):
            return
        
        self.__service.dispose()
