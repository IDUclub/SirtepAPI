import pickle
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Literal

import pandas as pd
from dateutil.parser import parse
from pydantic import BaseModel


class CachingService:
    """Class for caching interface"""

    def __init__(self, cache_path: Path) -> None:
        """
        Function initialize caching service class
        Args:
            cache_path: path to cache file
        Returns:
            None
        """

        self.caching_path = cache_path
        self.caching_path.mkdir(parents=True, exist_ok=True)
        self.cache_type: str

    @staticmethod
    def parse_arg(arg: int | str | bool | date | datetime) -> int | str:
        """
        Function parse arguments to appropriate format for parquet caching service.
        Args:
            arg (int | str | bool | date | datetime): arguments to parse
        Returns:
            int | str: parsed argument in str or int format
        Raises:
            TypeError: if argument is not supported for parsing
        """

        if isinstance(arg, bool):
            return str(arg).lower()
        elif isinstance(arg, date):
            return arg.strftime("%Y%m%d")
        elif isinstance(arg, datetime):
            return arg.strftime("%Y%m%d")
        else:
            raise TypeError(f"Argument {arg} is not supported")

    @staticmethod
    def load_arg(arg: str) -> int | str | bool | date | datetime:
        """
        Function load arguments to appropriate format for parquet caching service.
        Args:
            arg (str): arguments to parse from string
        Returns:
            int | str | bool | date | datetime
        Raises:
            ValueError: if argument is not supported for parsing
        """

        if arg.isdigit():
            return int(arg)
        elif arg == "true":
            return True
        elif arg == "false":
            return False
        try:
            return parse(arg)
        except Exception as e:
            raise ValueError(f"Argument {arg} is not supported") from e

    def create_cache_name(self, creation_date: date, *args) -> Path:
        """
        Function caches dataframe to feather. Dates parses in "YYYY-MM-DD" format.
        Args:
            creation_date (date): creation date
            *args (str | int | Bool | date | datetime): additional path argument.
            Date and DateTime are parsed in "YYYY-MM-DD" format.
        Returns:
            str: path to cached file
        """

        creation_date = creation_date.strftime("%Y-%m-%d")
        additional_args = [creation_date] + [self.parse_arg(arg) for arg in args]
        return self.caching_path.joinpath("_".join(additional_args))

    def check_file(self, file_name: str) -> tuple[bool, date | None]:
        """
        Function checks weather file exists and returns creation date if exists.
        Args:
            file_name (str): path to parquet file
        Returns:
            tuple[bool, date | None]: weather file exists or not and creation date if file exists
        """

        file_names = [file.name.split(".")[0] for file in self.caching_path.iterdir()]
        for file in file_names:
            if file_name in file:
                return True, parse(file.split("_")[0])

        return False, None

    @staticmethod
    def cache(func: Callable, cache_path: Path):
        """
        Function caches object with provided caching function
        Args:
            func (Callable): function to cache
            cache_path (Path): path to cache file
        Raises:
            SystemError: if error occurs during caching
        """

        try:
            func(cache_path)
            return cache_path
        except Exception as e:
            raise SystemError(
                f"Error caching object with cache_path {repr(cache_path)}"
            ) from e

    def cache_df(
        self,
        df: pd.DataFrame,
        cache_type: Literal["parquet", "pickle"],
        cache_variables: list[int | str | datetime | date],
    ) -> Path:
        """
        Function caches dataframe to feather. Dates parses in "YYYY-MM-DD" format.
        Args:
            df (pd.DataFrame): dataframe to cache
            cache_type: (Literal["parquet", "pickle"]): type of cache file
            cache_variables (list[int | str | datetime | date]): variables to create cache name.
        Returns:
            Path: path to cached file
        Raises:
            NotImplementedError: if file type is not supported
        """

        cache_path = self.create_cache_name(date.today(), *cache_variables)
        if cache_type == "parquet":
            return self.cache(df.to_parquet, cache_path)
        elif cache_type == "pickle":
            return self.cache(df.to_pickle, cache_path)
        else:
            raise NotImplementedError(f"File type {cache_type} is not supported")

    @staticmethod
    def read(func: Callable, file_path: Path) -> pd.DataFrame | BaseModel:
        """
        Function reads object from cache with provided reading function
        Args:
            func (Callable): function to read object from cache
            file_path (str): name of the file to read from cache
        Returns:
            pd.DataFrame | BaseModel: object read from cache
        Raises:
            SystemError: if function is not supported
        """

        try:
            return func(file_path)
        except Exception as e:
            raise SystemError(
                f"Error reading parquet file with path {file_path}"
            ) from e

    @staticmethod
    def read_internal_object(file_path: Path) -> BaseModel:
        """
        Function reads custom object from pickle file
        Args:
            file_path (Path): path to pickle
        Returns:
            BaseModel: object read from pickle
        """

        with open(file_path, "rb") as f:
            return pickle.load(f)

    def read_df(
        self,
        file_name: str,
        creation_date: datetime,
        file_type: Literal["parquet", "pickle"],
    ) -> pd.DataFrame:
        """
        Function reads dataframe from cache. Dates parses in "YYYY-MM-DD" format.
        Args:
            file_name (str): name of the file to read from cache
            creation_date (datetime): creation date
            file_type (Literal["parquet", "pickle"]): type of cache file
        Returns:
            pd.DataFrame: dataframe read from cache
        Raises:
            FileNotFoundError: if file is not found in cache
            NotImplementedError: if file type is not supported
        """

        file_name = "_".join([self.parse_arg(creation_date), file_name])
        file_names = [file.name for file in self.caching_path.iterdir()]
        for file in file_names:
            if file_name in file:
                if file_type == "parquet":
                    full_name = ".".join([file_name, "parquet"])
                    return self.read(
                        pd.read_parquet, self.caching_path.joinpath(full_name)
                    )
                elif file_type == "pickle":
                    full_name = ".".join([file_name, "parquet"])
                    return self.read(
                        pd.read_pickle, self.caching_path.joinpath(full_name)
                    )
                else:
                    raise NotImplementedError(f"File type {file_type} is not supported")
        raise FileNotFoundError(f"File {file_name} not found")

    def read_pydantic(
        self, file_name: str, creation_date: date, file_type: Literal["pickle"]
    ):

        file_name = "_".join([self.parse_arg(creation_date), file_name])
        file_names = [file.name for file in self.caching_path.iterdir()]
        for file in file_names:
            if file_name in file:
                if file_type == "pickle":
                    full_name = ".".join([file_name, "pickle"])
                    return self.read_internal_object(
                        self.caching_path.joinpath(full_name)
                    )
                else:
                    raise NotImplementedError(f"File type {file_type} is not supported")
