import json
import os
import typing
from pathlib import Path

def _get_real_path(base_path: str, relative: str) -> Path:
    if base_path is None:
        base_path_dir = os.getcwd()
    else:
        base_path_dir = os.path.dirname(base_path)
    
    return Path(os.path.realpath(os.path.join(base_path_dir, relative)))
    
class ConfigurationFile:
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

                self.__extends = ConfigurationFile(path=extends_path)
            else:
                raise ValueError("Invalid configuration file! The 'extends' property, if present, must be a string.")


    def _get_real_path(self, path: str) -> Path:
        return _get_real_path(self.__path, path)
        
    def has_value(self, name: typing.Union[str, typing.List[str]]) -> bool:
        return self.get_value(name) is not None
        
    def get_value(self, name: typing.Union[str, typing.List[str]]) -> typing.Any:
        val, _ = self.__get_value_with_source(name)

        return val

    def get_real_path_value(self, name: typing.Union[str, typing.List[str]]) -> Path or None:
        val, s = self.__get_value_with_source(name)

        if not val:
            return None

        return s._get_real_path(val)
        
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

class ParameterSchema:
    def __init__(self, name: str, d: dict) -> None:
        self.name = name
        self.required = d.get("required", True)
        self.type = d["type"]

class ParametersSchema:
    def __init__(self, d: typing.Dict[str, typing.Any]) -> None:
        self._data = d

    def items(self) -> typing.Iterable[ParameterSchema]:
        return (ParameterSchema(k, v) for k, v in self._data.items())

class DatasetSchema:
    def __init__(self, name: str, d: dict) -> None:
        self.name = name
        self.required = d.get("required", True)
        self.dataset_type = d.get("datasetType")
        self.item_schema = d.get("itemSchema")

class ServiceSchema:
    def __init__(self, schema_spec: dict) -> None:
        self._schema_spec = schema_spec

    def _get_parameters_schema(self, name: str):
        p: typing.Dict[str, dict] = self._schema_spec.get("parameters", dict())

        ret = p.get(name, dict())

        return ParametersSchema(ret)

    def _get_dataset_specs(self, direction: str) -> typing.Iterable[DatasetSchema]:
        if self._schema_spec is None:
            return list()

        datasets: typing.Dict[str, typing.Dict[str, dict]] = self._schema_spec.get("datasets")

        if datasets is None:
            return list()

        dataset_dict = datasets.get(direction)

        if dataset_dict is None:
            return list()

        return (DatasetSchema(k, v) for k, v in dataset_dict.items())
        
    def parameters_load(self):
        return self._get_parameters_schema("load")
        
    def parameters_process(self):
        return self._get_parameters_schema("process")

    def datasets_input(self):
        return self._get_dataset_specs("input")
        
    def datasets_output(self):
        return self._get_dataset_specs("output")

class ServiceInfo:
    def __init__(self, vals: dict) -> None:
        self.name = vals.get("name")
        self.version = vals.get("version")

class ServiceHostParameters:
    def __init__(self, vals: dict) -> None:
        self._vals = vals

    def get_host_config(self, name: str):
        d: dict = self._vals.get(name)

        if not d:
            return dict()

        return d

class ServiceParameters:
    def __init__(self, config: ConfigurationFile) -> None:
        self._config = config

    def get_value(self, name: str):
        return self._config.get_value(["parameters", name])

    def get_real_path_value(self, name: str):
        return self._config.get_real_path_value(["parameters", name])

class ServiceLoadParameters:
    def __init__(self, config: ConfigurationFile) -> None:
        self.module_path = config.get_real_path_value("modulePath")
        self.module_name = config.get_value("moduleName")
        self.package_name = config.get_value("packageName")

        if self.module_name and self.module_path:
            raise ValueError("Both modulePath and moduleName were specified!")
        elif not self.module_name and not self.module_path:
            raise ValueError("The module couldn't be determined! Please include either a modulePath or moduleName in configuration.")

        self.class_name = config.get_value("className")
        self.service_instance_name = config.get_value("serviceInstanceName")

        if self.class_name and self.service_instance_name:
            raise ValueError("Both className and serviceInstanceName were specified!")
        elif not self.class_name and not self.service_instance_name:
            raise ValueError("Either className or serviceInstanceName must be specified!")


T = typing.TypeVar("T")
class ServiceConfiguration:
    def __init__(self, config_file: typing.Union[str, ConfigurationFile]) -> None:
        if isinstance(config_file, str):
            config_file = ConfigurationFile(config_file)
        
        self._config = config_file

    def _has(self, name: str):
        d = self._config.get_value(name)

        return d is not None

    def _get_typed(self, type: typing.Callable[[dict], T], name: str, coalesce = False) -> T:
        d = self._config.get_value(name)

        if not d:
            if coalesce:
                return type(dict())
            else:
                return None

        return type(d)

    def service(self):
        return ServiceLoadParameters(self._config)

    def schema(self):
        return self._get_typed(ServiceSchema, "schema", coalesce=True)

    def info(self):
        return self._get_typed(ServiceInfo, "info", coalesce=True)

    def host(self):
        return self._get_typed(ServiceHostParameters, "host", coalesce=True)

    def has_parameters(self):
        return self._has("parameters")

    def parameters(self):
        return ServiceParameters(self._config)
