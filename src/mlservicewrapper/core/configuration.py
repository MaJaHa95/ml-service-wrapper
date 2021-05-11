import typing

from mlservicewrapper import config


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
    def __init__(self, schema_spec: config.ConfigurationPart) -> None:
        self._schema_spec = schema_spec

    def _get_parameters_schema(self, name: str):
        p: typing.Dict[str, dict] = self._schema_spec.get_dict_value("parameters") or dict()

        ret = p.get(name, dict())

        return ParametersSchema(ret)

    def _get_dataset_specs(self, direction: str) -> typing.Iterable[DatasetSchema]:
        if self._schema_spec is None:
            return list()

        datasets: typing.Dict[str, typing.Dict[str, dict]] = self._schema_spec.get_dict_value("datasets") or dict()

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
    def __init__(self, vals: config.ConfigurationPart) -> None:
        self.name = vals.get_value("name")
        self.version = vals.get_value("version")

class ServiceHostParameters:
    def __init__(self, vals: config.ConfigurationPart) -> None:
        self._vals = vals

    def get_host_config(self, name: str):
        return self._vals.get_sub_config(name, is_nested=True, required=False)

class ServiceParameters:
    def __init__(self, config_part: config.ConfigurationPart) -> None:
        self._config = config_part

    def get_value(self, name: str):
        return self._config.get_value(name)

    def get_real_path_value(self, name: str):
        return self._config.get_fully_qualified_path(name)

class ServiceLoadParameters:
    def __init__(self, config: config.ConfigurationPart) -> None:
        self.module_path = config.get_fully_qualified_path("modulePath")
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
    def __init__(self, config_file: str, host_type: str) -> None:
        self._config = config.from_file(config_file)

        if host_type:
            hosts = self._config.get_sub_config("host", is_nested=True, required=False)

            self._config = hosts.get_sub_config(host_type, is_nested=True, required=False)

    def _get_typed(self, type: typing.Callable[[config.ConfigurationPart], T], name: str, coalesce = False) -> T:
        d = self._config.get_sub_config(name)

        if d:
            return type(d)
        if coalesce:
            return type(config.empty())
        else:
            return None

    def plugins(self):
        parts = self._config.get_multi_sub_config("plugins")

        for part in parts:
            yield ServiceLoadParameters(part)

    def service(self):
        return ServiceLoadParameters(self._config)

    def schema(self):
        return self._get_typed(ServiceSchema, "schema", coalesce=True)

    def info(self):
        return self._get_typed(ServiceInfo, "info", coalesce=True)

    def host(self):
        return self._get_typed(ServiceHostParameters, "host", coalesce=True)

    def parameters(self):
        return ServiceParameters(self._config.get_sub_config("parameters"))
