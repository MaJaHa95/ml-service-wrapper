import importlib
import os
import typing

import mlservicewrapper.core.services as services
from mlservicewrapper.core.configuration import ServiceLoadParameters


class _BaseServiceModuleLoader:
    def load(self):
        raise NotImplementedError()

class _PathServiceModuleLoader(_BaseServiceModuleLoader):
    def __init__(self, path: str) -> None:
        super().__init__()

        self.__path = path

    def load(self):
        
        print("Loading from script {}".format(self.__path))

        dirname = os.path.dirname(self.__path)
        basename = os.path.basename(self.__path)

        os.sys.path.insert(0, dirname)

        module_name = os.path.splitext(basename)[0]

        print("Importing module {} from {}...".format(module_name, dirname))

        return importlib.import_module(module_name)

class _PackageServiceModuleLoader(_BaseServiceModuleLoader):
    def __init__(self, module_name: str, package_name: str) -> None:
        super().__init__()

        self.__module_name = module_name
        self.__package_name = package_name

    def load(self):
        if self.__package_name:
            print("Importing module {} from {}...".format(self.__module_name, self.__package_name))
        else:
            print("Importing module {}...".format(self.__module_name))

        return importlib.import_module(self.__module_name, self.__package_name)

def _get_service_module_loader(config: ServiceLoadParameters) -> _BaseServiceModuleLoader:
    if config.module_path:
        return _PathServiceModuleLoader(config.module_path)

    elif config.module_name:
        return _PackageServiceModuleLoader(config.module_name, config.package_name)

    else:
        raise ValueError("The module couldn't be determined! Please include either a modulePath or moduleName in configuration.")

class BaseServiceLoader:
    def __init__(self, module_loader: _BaseServiceModuleLoader) -> None:
        self.__module_loader = module_loader
    
    def _get_instance_from_module(self, service_module) -> services.Service:
        raise NotImplementedError()
    
    def get_instance(self):
        module = self.__module_loader.load()
        
        print("Imported module")

        return self._get_instance_from_module(module)

    def get_name(self):
        raise NotImplementedError()

class _ClassServiceLoader(BaseServiceLoader):
    def __init__(self, module_loader: _BaseServiceModuleLoader, class_name: str, args: typing.List[typing.Any] = None, kwds: typing.Dict[str, typing.Any] = None) -> None:
        super().__init__(module_loader)

        self._class_name = class_name

        self._args = args or list()
        self._kwds = kwds or dict()

    def _get_instance_from_module(self, service_module) -> services.Service:
        service_type = getattr(service_module, self._class_name)

        print("Identified service type: {}".format(str(service_type)))

        return service_type(*self._args, **self._kwds)

    def get_name(self):
        return self._class_name

class _InstanceServiceLoader(BaseServiceLoader):
    def __init__(self, module_loader: _BaseServiceModuleLoader, instance_name: str) -> None:
        super().__init__(module_loader)

        self._instance_name = instance_name

    def _get_instance_from_module(self, service_module) -> services.Service:

        return getattr(service_module, self._instance_name)

    def get_name(self):
        return self._instance_name


def get_service_loader(config: ServiceLoadParameters, args: typing.List[typing.Any] = None, kwds: typing.Dict[str, typing.Any] = None) -> BaseServiceLoader:

    module_loader = _get_service_module_loader(config)
    
    if config.class_name:
        return _ClassServiceLoader(module_loader, config.class_name, args, kwds)

    elif config.service_instance_name:
        return _InstanceServiceLoader(module_loader, config.service_instance_name)
    else:
        raise ValueError("Either className or serviceInstanceName must be specified!")
