import asyncio
import logging
import math
import os
import statistics
import time
import typing

import pandas as pd
from mlservicewrapper.core import context_sources, server
from mlservicewrapper.core.debug.file_lookups import FileDatasetLookup
from mlservicewrapper.core.debug.profiling import (BaseProfilerWrapper,
                                                   get_profiler)

from .. import contexts, errors, services


def _print_ascii_histogram(seq: typing.List[float]) -> None:
    """A horizontal frequency-table/histogram plot."""

    hist = {}

    _min = min(seq)
    _max = max(seq)
    _len = len(seq)

    buckets = 10
    step = (_max - _min) / (buckets - 1)

    for i in seq:
        e = _min + (math.floor((i - _min) / step) * step)

        hist[e] = hist.get(e, 0) + 1

    for i in range(buckets):
        e = _min + (i * step)

        ct = hist.get(e, 0)

        pct = ct / _len

        w = math.floor(40 * pct)

        if ct > 0:
            w = max(w, 1)

        print('{0:5f}s {1}'.format(e, '+' * w))

def get_input_dataframe(name: str, file_lookup: FileDatasetLookup) -> pd.DataFrame:
    context_sources.NameValidator.raise_if_invalid(name)

    file_handler = file_lookup.get_file_handler(name)

    return file_handler.read_dataframe()

class _LocalRunContextSource(context_sources.CollectingProcessContextSource):
    def __init__(self, input_datasets: FileDatasetLookup, output_datasets: FileDatasetLookup = None, parameters: dict = None):
        super().__init__()
        self.__parameters = parameters or dict()

        self.input_datasets = input_datasets
        self.output_datasets = output_datasets

    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        context_sources.NameValidator.raise_if_invalid(name)
        
        if name in self.__parameters:
            return self.__parameters[name]
        
        if required and default is None:
            raise errors.MissingParameterError(name)

        return default

    async def get_input_dataframe(self, name: str, required: bool = True):
        df = get_input_dataframe(name, self.input_datasets)
        
        if required and df is None:
            raise errors.MissingDatasetError(name)

        return df

    async def set_output_dataframe(self, name: str, df: pd.DataFrame):
        context_sources.NameValidator.raise_if_invalid(name)

        await super().set_output_dataframe(name, df)

        handler = self.output_datasets.get_file_handler(name, "csv", must_exist=False)
        
        if not handler:
            return

        d = os.path.dirname(handler.path)
        os.makedirs(d, exist_ok=True)

        handler.save_dataframe(df)

class _LocalDataFrameRunContextSource(context_sources.ProcessContextSource):
    def __init__(self, df: pd.DataFrame, name: str, base_ctx: context_sources.ProcessContextSource):
        self.__base_ctx = base_ctx
        self.__name = name
        self.__df = df

    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        return self.__base_ctx.get_parameter_value(name, required, default)

    def set_output_dataframe(self, name: str, df: pd.DataFrame):
        return self.__base_ctx.set_output_dataframe(name, df)

    async def get_input_dataframe(self, name: str, required: bool = True):
        context_sources.NameValidator.raise_if_invalid(name)
        
        if name == self.__name:
            return self.__df

        return await self.__base_ctx.get_input_dataframe(name, required)

async def _perform_accuracy_assessment(ctx: context_sources.CollectingProcessContextSource, specs: typing.Dict[str, str]):

    for k, v in specs.items():
        i = k.split(".")
        o = v.split(".")

        input_df = await ctx.get_input_dataframe(i[0], required=True)
        output_df = ctx.get_output_dataframe(o[0])

        input_field = input_df[i[1]]
        input_field.name = "Expected"

        output_field = output_df[o[1]]
        output_field.name = "Actual"

        joined = output_field.to_frame().join(input_field, how="inner")

        joined["Result"] = joined["Actual"] == joined["Expected"]

        count_total = len(joined.index)
        count_correct = joined["Result"].values.sum()

        print("Accuracy ({} to {}): {} of {} ({})".format(k, v, count_correct, count_total, count_correct / count_total))

class _ServiceProcessTimer:
    def __init__(self, server_instance: server.ServerInstance, profiler: "BaseProfilerWrapper") -> None:
        self._times: typing.List[float] = list()

        self._server_instance = server_instance
        self._profiler = profiler

    async def process(self, ctx: context_sources.ProcessContextSource):
        
        s = time.perf_counter()
        self._profiler.enable()
        await self._server_instance.process(ctx)
        self._profiler.disable()
        e = time.perf_counter()

        self._times.append(e - s)

    def print_stats(self):
        if len(self._times) == 0:
            print("Count: 0")
        elif len(self._times) == 1:
            print("Process time: {}s".format(self._times[0]))
        else:
            print()
            print("Count: {}".format(len(self._times)))
            print("Min process time: {}s".format(min(self._times)))
            print("Mean process time: {}s".format(statistics.mean(self._times)))
            print("Median process time: {}s".format(statistics.median(self._times)))
            print("Max process time: {}s".format(max(self._times)))

            _print_ascii_histogram(self._times)

def _get_run_contexts(run_context: _LocalRunContextSource, split_dataset_name: str):
    df = get_input_dataframe(split_dataset_name, run_context.input_datasets)
    
    for _, r in df.iterrows():
        rdf = pd.DataFrame([r]).reset_index(drop=True)
    
        row_run_context = _LocalDataFrameRunContextSource(rdf, split_dataset_name, run_context)

        yield row_run_context

async def _call_load(server_instance: server.ServerInstance, load_context_source: context_sources.ServiceContextSource = None, load_context_override: dict = None):
    print("Loading...")
    s = time.perf_counter()
    await server_instance.load(load_context_source, override=load_context_override)
    e = time.perf_counter()

    load_time = e - s
    
    return load_time

def _get_run_context(input_dataset_paths: typing.Dict[str, str] = None, input_dataset_directory: str = None, output_dataset_directory: str = None, output_dataset_paths: typing.Dict[str, str] = None, runtime_parameters: dict = None):
    
    input_datasets = FileDatasetLookup(input_dataset_directory, input_dataset_paths)
    output_datasets = FileDatasetLookup(output_dataset_directory, output_dataset_paths)

    return _LocalRunContextSource(input_datasets, output_datasets, runtime_parameters)


async def run_async(server_instance: server.ServerInstance, load_context_source: context_sources.ServiceContextSource = None, load_context_override: dict = None, input_dataset_paths: typing.Dict[str, str] = None, input_dataset_directory: str = None, output_dataset_directory: str = None, output_dataset_paths: typing.Dict[str, str] = None, split_dataset_name: str = None, runtime_parameters: dict = None, assess_accuracy: dict = None, profile_processing_to_file: str = None):
    if input_dataset_paths is None and input_dataset_directory is None:
        logging.warn("Neither input_dataset_paths nor input_dataset_directory was specified, meaning input datasets will not be available!")
    
    load_time = await _call_load(server_instance, load_context_source, load_context_override)
    
    print("Running...")

    run_context_source = _get_run_context(input_dataset_paths, input_dataset_directory, output_dataset_directory, output_dataset_paths, runtime_parameters)

    profiler = get_profiler(profile_processing_to_file)

    process_timer = _ServiceProcessTimer(server_instance, profiler)

    if split_dataset_name:
        for ctx in _get_run_contexts(run_context_source, split_dataset_name):
            await process_timer.process(ctx)
    else:
        await process_timer.process(run_context_source)

    profiler.save()

    print("Load time: {}s".format(load_time))
    process_timer.print_stats()

    result = dict(run_context_source.output_dataframes())

    if assess_accuracy is not None:
        await _perform_accuracy_assessment(run_context_source, assess_accuracy)

    return result

def run(service: typing.Union[services.Service, typing.Callable], **kwargs):

    loop = asyncio.get_event_loop()

    return loop.run_until_complete(run_async(service, **kwargs))
