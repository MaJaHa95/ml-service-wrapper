
import typing

import os

import pandas as pd

class BaseFileHandler:
    def __init__(self, path: os.PathLike) -> None:
        self.path = path

    def read_dataframe(self) -> pd.DataFrame:
        raise NotImplementedError()

    def save_dataframe(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()

class _CsvFileHandler(BaseFileHandler):
    def __init__(self, path: os.PathLike) -> None:
        super().__init__(path)

    def read_dataframe(self) -> pd.DataFrame:
        return pd.read_csv(self.path, keep_default_na=False)

    def save_dataframe(self, df: pd.DataFrame) -> None:
        return df.to_csv(self.path, index=False)
        
class _ExcelFileHandler(BaseFileHandler):
    def __init__(self, path: os.PathLike) -> None:
        super().__init__(path)

    def read_dataframe(self) -> pd.DataFrame:
        return pd.read_excel(self.path, keep_default_na=False)

    def save_dataframe(self, df: pd.DataFrame) -> None:
        return df.to_excel(self.path, index=False)

class FileDatasetLookup:
    def __init__(self, directory: str, path_map: typing.Dict[str, str]):
        self.__directory = directory
        self.__map = path_map

        self.__handler_maps: typing.Dict[os.PathLike, typing.Callable[[str], BaseFileHandler]] = {
            "csv": _CsvFileHandler,
            "xlsx": _ExcelFileHandler
        }

    def allowed_file_extensions(self) -> typing.Iterable[str]:
        return self.__handler_maps.keys()

    def _get_file_handler_from_path(self, path: os.PathLike) -> BaseFileHandler:
        _, ext = os.path.splitext(path)

        handler = self.__handler_maps.get(ext[1:])

        if not handler:
            raise NotImplementedError(f"File extension '{ext}' is not known!")
            
        return handler(path)

    def get_file_handler(self, name: str, default_extension: str or None = None, must_exist = True):
        if self.__map is not None and name in self.__map:
            return self._get_file_handler_from_path(self.__map[name])

        if self.__directory is None:
            return None

        if default_extension:
            if default_extension not in self.__handler_maps:
                raise ValueError("Default extension '{}' is not known!".format(default_extension))

            exts = [default_extension, *self.allowed_file_extensions()]
        else:
            exts = self.allowed_file_extensions()

        for ext in exts:
            path = os.path.join(self.__directory, name + os.path.extsep + ext)

            if path is None:
                continue

            if must_exist and not os.path.exists(path):
                continue

            return self._get_file_handler_from_path(path)

        return None
