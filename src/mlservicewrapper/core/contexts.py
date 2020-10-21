import asyncio
import os
import re
import types
import typing
from pathlib import Path

import pandas as pd

from . import errors


class NameValidator:
    __valid_name_regex = re.compile(r'^[A-Z][a-zA-Z\d]*$')

    @classmethod
    def raise_if_invalid(cls, name: str):
        if not cls.is_valid(name):
            raise ValueError("Name is not valid: '{}'!".format(name))
    
    @classmethod
    def is_valid(cls, name: str):
        return cls.__valid_name_regex.match(name) is not None
    
__all__ = ["NameValidator", "ServiceContext", "ProcessContext", "CollectingProcessContext", "EnvironmentVariableServiceContext", "CoalescingServiceContext"]

class ServiceContext:
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        raise NotImplementedError()

    def get_parameter_real_path_value(self, name: str, required: bool = True) -> Path:
        NameValidator.raise_if_invalid(name)

        val = self.get_parameter_value(name, required)

        if val is None:
            return val

        return os.path.join(os.getcwd(), val)
        
class ProcessContext:
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        raise NotImplementedError()
    
    async def get_input_dataframe(self, name: str, required: bool = True):
        raise NotImplementedError()

    async def set_output_dataframe(self, name: str, df: pd.DataFrame):
        raise NotImplementedError()
        
class CollectingProcessContext(ProcessContext):
    def __init__(self):
        super()
        self.__output_dataframes = dict()

    async def set_output_dataframe(self, name: str, df: pd.DataFrame):
        NameValidator.raise_if_invalid(name)

        self.__output_dataframes[name] = df
    
    def get_output_dataframe(self, name: str):
        NameValidator.raise_if_invalid(name)

        return self.__output_dataframes.get(name)

    def output_dataframes(self):
        return self.__output_dataframes.items()

class CoalescingServiceContext(ServiceContext):
    def __init__(self, contexts: typing.List[ServiceContext]):
        self.__contexts = contexts
    
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        NameValidator.raise_if_invalid(name)

        for context in self.__contexts:
            val = context.get_parameter_value(name, False)

            if val is not None:
                return val
            
        if required and default is None:
            raise errors.MissingParameterError(name)

        return default

    def get_parameter_real_path_value(self, name: str, required: bool = True) -> Path:
        NameValidator.raise_if_invalid(name)

        for context in self.__contexts:
            val = context.get_parameter_real_path_value(name, False)

            if val is not None:
                return val
            
        if required:
            raise errors.MissingParameterError(name)

        return None

class DictServiceContext(ServiceContext):
    def __init__(self, values: dict):
        self.__values = values
    
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        NameValidator.raise_if_invalid(name)

        ev = self.__values.get(name) or default

        if required and not ev:
            raise errors.MissingParameterError(name)

        return ev


class EnvironmentVariableServiceContext(ServiceContext):
    def __init__(self, prefix: str):
        self.__prefix = prefix
    
    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        NameValidator.raise_if_invalid(name)

        ev = os.environ.get(self.__prefix + name) or default

        if required and not ev:
            raise errors.MissingParameterError(name)

        return ev
