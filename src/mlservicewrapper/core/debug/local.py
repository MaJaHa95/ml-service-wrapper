import asyncio
import json
import logging
import math
import os
import re
import statistics
import time
import typing

import pandas as pd

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

class _FileDatasetLookup:
    def __init__(self, directory: str, path_map: typing.Dict[str, str]):
        self.__directory = directory
        self.__map = path_map

    def get_path(self, name: str, extension: str):
        if self.__map is not None and name in self.__map:
            return self.__map[name]

        if self.__directory is not None:
            return os.path.join(self.__directory, name + "." + extension)

        return None

def get_input_dataframe(name: str, file_lookup: _FileDatasetLookup) -> pd.DataFrame:
    contexts.NameValidator.raise_if_invalid(name)

    file_path = file_lookup.get_path(name, "csv")

    if file_path:
        return pd.read_csv(file_path, keep_default_na=False)

    return None

class _LocalRunContext(contexts.CollectingProcessContext):
    def __init__(self, input_datasets: _FileDatasetLookup, output_datasets: _FileDatasetLookup = None, parameters: dict = None):
        super().__init__()
        self.__parameters = parameters or dict()

        self.__input_datasets = input_datasets
        self.__output_datasets = output_datasets

    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        contexts.NameValidator.raise_if_invalid(name)
        
        if name in self.__parameters:
            return self.__parameters[name]
        
        if required and default is None:
            raise errors.MissingParameterError(name)

        return default

    async def get_input_dataframe(self, name: str, required: bool = True):
        df = get_input_dataframe(name, self.__input_datasets)
        
        if required and df is None:
            raise errors.MissingDatasetError(name)

        return df

    async def set_output_dataframe(self, name: str, df: pd.DataFrame):
        contexts.NameValidator.raise_if_invalid(name)

        await super().set_output_dataframe(name, df)
        
        # print("Got results for {}".format(name))
        # print(df)
        # print()

        path = self.__output_datasets.get_path(name, "csv")
        
        if path:
            dir = os.path.dirname(path)
            os.makedirs(dir, exist_ok=True)

            df.to_csv(path, index=False)

class _LocalDataFrameRunContext(contexts.ProcessContext):
    def __init__(self, df: pd.DataFrame, name: str, base_ctx: contexts.ProcessContext):
        self.__base_ctx = base_ctx
        self.__name = name
        self.__df = df

    def get_parameter_value(self, name: str, required: bool = True, default: str = None) -> str:
        return self.__base_ctx.get_parameter_value(name, required, default)

    def set_output_dataframe(self, name: str, df: pd.DataFrame):
        return self.__base_ctx.set_output_dataframe(name, df)

    async def get_input_dataframe(self, name: str, required: bool = True):
        contexts.NameValidator.raise_if_invalid(name)
        
        if name == self.__name:
            return self.__df

        return await self.__base_ctx.get_input_dataframe(name, required)

    def output_dataframes(self):
        return self.__base_ctx.output_dataframes()

async def _perform_accuracy_assessment(ctx: contexts.CollectingProcessContext, specs: dict):

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

async def run_async(service: typing.Union[services.Service, typing.Callable], load_context: contexts.ServiceContext = None, input_dataset_paths: typing.Dict[str, str] = None, input_dataset_directory: str = None, output_dataset_directory: str = None, output_dataset_paths: typing.Dict[str, str] = None, split_dataset_name: str = None, runtime_parameters: dict = None, assess_accuracy: dict = None, profile_processing_to_file: str = None):
    if input_dataset_paths is None and input_dataset_directory is None:
        logging.warn("Neither input_dataset_paths nor input_dataset_directory was specified, meaning input datasets will not be available!")
    
    if callable(service):
        service = service()
        initialized_service = True
    else:
        initialized_service = False

    if load_context is None:
        load_context = contexts.DictServiceContext(dict())

    if hasattr(service, 'load'):
        print("Loading...")
        s = time.perf_counter()
        await service.load(load_context)
        e = time.perf_counter()

        load_time = e - s
    else:
        load_time = 0
    
    print("Running...")

    input_datasets = _FileDatasetLookup(input_dataset_directory, input_dataset_paths)
    output_datasets = _FileDatasetLookup(output_dataset_directory, output_dataset_paths)

    run_context = _LocalRunContext(input_datasets, output_datasets, runtime_parameters)

    if profile_processing_to_file is None:
        from types import SimpleNamespace
        
        nop = lambda *a, **k: None
        
        p = SimpleNamespace(enable=nop, disable=nop, dump_stats=nop)
    else:
        import cProfile

        p = cProfile.Profile()

    times = list()

    if split_dataset_name:
        df = get_input_dataframe(split_dataset_name, input_datasets)
        
        for _, r in df.iterrows():
            rdf = pd.DataFrame([r]).reset_index(drop=True)
        
            row_run_context = _LocalDataFrameRunContext(rdf, split_dataset_name, run_context)

            s = time.perf_counter()
            p.enable()
            await service.process(row_run_context)
            p.disable()
            e = time.perf_counter()

            times.append(e - s)
    else:

        s = time.perf_counter()
        p.enable()
        await service.process(run_context)
        p.disable()
        e = time.perf_counter()

        times.append(e - s)

    p.dump_stats(profile_processing_to_file)

        #cProfile.runctx('_run_processing(service, times, input_datasets, run_context, split_dataset_name)', globals(), locals(), filename=profile_processing_to_file)

    print("Load time: {}s".format(load_time))
    if len(times) == 0:
        print("Count: 0")
    elif len(times) == 1:
        print("Process time: {}s".format(times[0]))
    else:
        print()
        print("Count: {}".format(len(times)))
        print("Min process time: {}s".format(min(times)))
        print("Mean process time: {}s".format(statistics.mean(times)))
        print("Median process time: {}s".format(statistics.median(times)))
        print("Max process time: {}s".format(max(times)))

        _print_ascii_histogram(times)

    if initialized_service and hasattr(service, 'dispose'):
        service.dispose()
    
    result = dict(run_context.output_dataframes())

    if assess_accuracy is not None:
        await _perform_accuracy_assessment(run_context, assess_accuracy)

    return result

def run(service: typing.Union[services.Service, typing.Callable], **kwargs):

    loop = asyncio.get_event_loop()

    return loop.run_until_complete(run_async(service, **kwargs))
