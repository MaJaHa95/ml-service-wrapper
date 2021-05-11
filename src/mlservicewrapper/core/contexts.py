import logging
from pathlib import Path

import pandas as pd

from . import context_sources

__all__ = ["ServiceContext", "ProcessContext"]

        
class ServiceContext(context_sources.ServiceContextSource):
    def __init__(self, context_source: context_sources.ServiceContextSource, logger: logging.Logger) -> None:
        super().__init__()

        self._context_source = context_source
        self._logger = logger

    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        return self._context_source.get_parameter_value(name, required=required, default=default)

    def get_parameter_real_path_value(self, name: str, required: bool = True) -> Path:
        return self._context_source.get_parameter_real_path_value(name, required=required)
    
    @property
    def logger(self):
        return self._logger
    
class ProcessContext(context_sources.ProcessContextSource):
    def __init__(self, context_source: context_sources.ProcessContextSource, logger: logging.Logger) -> None:
        super().__init__()

        self._context_source = context_source
        self._logger = logger

    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        return self._context_source.get_parameter_value(name, required=required, default=default)
    
    def get_input_dataframe(self, name: str, required: bool = True) -> pd.DataFrame:
        return self._context_source.get_input_dataframe(name, required=required)

    def set_output_dataframe(self, name: str, df: pd.DataFrame):
        return self._context_source.set_output_dataframe(name, df)

    @property
    def logger(self):
        return self._logger
    