from pathlib import Path
from typing import Literal

import pandas as pd
from iduconfig import Config

from app.common.exceptions.http_exception_wrapper import http_exception

from .caching.parquet_caching_service import ParquetCachingService


class StorageService:
    """
    Storage service class for data caching and retrieval
    Attributes:
        config (Config): instance of Config class from iduconfig
        parquet_caching_service (ParquetCachingService): service for caching and retrieving data in feather format
    """

    def __init__(self, config: Config, parquet_caching_service: ParquetCachingService):
        """
        Initializes StorageService.
        Args:
            config (Config): instance of Config class from iduconfig
            parquet_caching_service (ParquetCachingService): service for caching and retrieving data in feather format
        """
        self.config = config
        self.parquet_caching_service = parquet_caching_service

    def cache_df(self, df: pd.DataFrame, extension: Literal["parquet"], *args) -> Path:
        """
        Function to cache dataframe to parquet
        Args:
            df (pd.DataFrame): dataframe to cache
            extension (Literal["parquet"]): file extension for caching, currently only "parquet" is supported
            *args: variable length argument list for df_to_parquet method of ParquetCachingService
        Returns:
            Path: path to cached file
        Raises:
            500 HTTPException: if error occurs during caching or unsupported file extension is provided
        """

        if extension == "parquet":
            try:
                return self.parquet_caching_service.cache_df(df, *args)
            except Exception as e:
                raise http_exception(
                    500,
                    f"Error caching dataframe to parquet: {repr(e)}",
                    _input={"args": args, "extension": extension},
                    _detail={"error": repr(e)},
                ) from e
        else:
            raise NotImplementedError(
                f"Unsupported file extension: {repr(extension)}, only parquet is supported for caching"
            )
