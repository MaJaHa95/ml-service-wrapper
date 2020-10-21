import importlib.util
import inspect
import json
import os
import sys
import typing
from pathlib import Path

from . import contexts, errors, services

__all__ = ["ServiceConfiguration", "ServerInstance"]

def _get_real_path(base_path: str, relative: str) -> Path:
    if base_path is None:
        base_path_dir = os.getcwd()
    else:
        base_path_dir = os.path.dirname(base_path)
    
    return Path(os.path.realpath(os.path.join(base_path_dir, relative)))
    
class ServiceConfiguration:
    def __init__(self, path: str = None, config: dict = None):
        if path is None:
            self.__config = config.copy()
        else:
            self.__path = path
                
            with open(path, "r") as config_file:
                self.__config = json.load(config_file)
            
        extends_path = self.__config.get("extends")

        if extends_path is None:
            self.__extends = None
        else:
            if isinstance(extends_path, str):
                extends_path = _get_real_path(self.__path, extends_path)

                self.__extends = ServiceConfiguration(path=extends_path)
            else:
                raise ValueError("Invalid configuration file! The 'extends' property, if present, must be a string.")


    def __get_real_path(self, path: str) -> Path:
        return _get_real_path(self.__path, path)
        
    def has_value(self, name: typing.Union[str, typing.List[str]]) -> bool:
        return self.get_value(name) is not None
        
    def get_value(self, name: typing.Union[str, typing.List[str]]) -> typing.Any:
        val, _ = self.__get_value_with_source(name)

        return val

    def get_real_path_value(self, name: typing.Union[str, typing.List[str]]) -> Path:
        val, s = self.__get_value_with_source(name)

        return s.__get_real_path(val)
        
    def __get_value_with_source(self, name: typing.Union[str, typing.List[str]]):
        if isinstance(name, str):
            result = self.__config.get(name)
        else:
            if name is None or len(name) == 0:
                raise ValueError("Provide at least one name part")
            
            result = self.__config
            for part in name:
                result = result.get(part)

                if result is None:
                    break
        
        if result is None and self.__extends is not None:
            return self.__extends.__get_value_with_source(name)

        return result, self

class _ConfigurationFileServiceContext(contexts.ServiceContext):
    def __init__(self, config: ServiceConfiguration):
        self.__config = config
    
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        contexts.NameValidator.raise_if_invalid(name)

        val = self.__config.get_value(["parameters", name]) or default
        
        if required and val is None:
            raise errors.MissingParameterError(name)

        return val

    def get_parameter_real_path_value(self, name: str, required: bool = True) -> Path:
        contexts.NameValidator.raise_if_invalid(name)

        val = self.__config.get_real_path_value(["parameters", name])
        
        if required and val is None:
            raise errors.MissingParameterError(name)

        return val
        

class ServerInstance:
    def __init__(self, config_path: str = None):

        if not config_path:
            config_path = os.environ.get("SERVICE_CONFIG_PATH", "./service/config.json")

        self.__config = ServiceConfiguration(config_path)

        self.__service_module_path = self.__config.get_real_path_value("modulePath")

        if self.__service_module_path is None:
            raise ValueError("The modulePath couldn't be determined!")

        self.__class_name = self.__config.get_value("className")
        self.__service_instance_name = self.__config.get_value("serviceInstanceName")

        if self.__class_name is None and self.__service_instance_name is None:
            raise ValueError("Either className or serviceInstanceName must be specified in the configuration file!")

        self.__host_configs = self.__config.get_value("host")

        self.__schema = self.__config.get_value("schema")
        self.__info = self.__config.get_value("info")
        
        self.__service: services.Service = None

    def get_info(self) -> dict:
        return self.__info

    def __get_parameters_specs(self, step: str) -> typing.Dict[str, dict]:
        if self.__schema is None:
            return dict()

        datasets = self.__schema.get("parameters")

        if datasets is None:
            return dict()

        return datasets.get(step, dict())
        
    def get_load_parameter_specs(self) -> typing.Dict[str, dict]:
        return self.__get_parameters_specs("load")
        
    def get_process_parameter_specs(self) -> typing.Dict[str, dict]:
        return self.__get_parameters_specs("process")
        
    def __get_dataset_specs(self, direction: str) -> typing.Dict[str, dict]:
        if self.__schema is None:
            return dict()

        datasets = self.__schema.get("datasets")

        if datasets is None:
            return dict()

        return datasets.get(direction, dict())
        
    def get_input_dataset_specs(self) -> typing.Dict[str, dict]:
        return self.__get_dataset_specs("input")

    def get_output_dataset_specs(self) -> typing.Dict[str, dict]:
        return self.__get_dataset_specs("output")

    def get_host_config_section(self, name: str) -> dict:
        if self.__host_configs is None:
            return None
        
        return self.__host_configs.get(name)

    def get_parameter_real_path_value(self, name: str) -> str:
        return self.__config.get_real_path_value(["parameters", name])
    
    def __get_service_instance(self) -> services.Service:

        print("Loading from script {}".format(self.__service_module_path))

        service_module_dirname = os.path.dirname(self.__service_module_path)
        service_module_basename = os.path.basename(self.__service_module_path)

        os.sys.path.insert(0, service_module_dirname)

        service_module_name = os.path.splitext(service_module_basename)[0]

        print("Importing module {} from {}...".format(service_module_name, service_module_dirname))

        service_module = importlib.import_module(service_module_name)

        print("Imported module")

        if self.__class_name is not None:
            service_type = getattr(service_module, self.__class_name)

            print("Identified service type: {}".format(str(service_type)))

            service = service_type()
        else:
            service = getattr(service_module, self.__service_instance_name)

        print("Got service: {}".format(service))

        return service

    def is_loaded(self):
        return self.__service is not None

    def build_context(self, include_environment_variables = True, override: dict = None):
        context_parts = []
        
        if override is not None:
            context_parts.append(contexts.DictServiceContext(override))

        if include_environment_variables:
            context_parts.append(contexts.EnvironmentVariableServiceContext("SERVICE_"))
        
        if self.__config.has_value("parameters"):
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
