from datetime import datetime
from typing import Literal

import pandas as pd
from iduconfig import Config
from pydantic import BaseModel

from .cacheable import CacheableDF, CacheablePydantic
from .sirtep_storage import SirtepStorage


class StorageService:
    """
    Storage service class for data caching and retrieval
    Attributes:
        config (Config): instance of Config class from iduconfig
        matrix_storage
    """

    def __init__(
        self,
        config: Config,
        matrix_storage: SirtepStorage,
        response_storage: SirtepStorage,
        provision_storage: SirtepStorage,
    ):
        """
        Initializes StorageService.
        Args:
            config (Config): instance of Config class from iduconfig
            matrix_storage (Storage): instance of Storage class from idustorage
            response_storage (Storage): instance of Storage class from idustorage
            provision_storage (Storage): instance of Storage class from idustorage
        """

        self.config = config
        self.matrix_storage = matrix_storage
        self.response_storage = response_storage
        self.provision_storage = provision_storage

    def get_storage(
        self, dt_type: Literal["matrix", "provision", "response"]
    ) -> SirtepStorage:
        """
        Retrieves the appropriate storage based on the DataFrame type.
        Args:
            dt_type (Literal["matrix", "provision", "response"]): Type of DataFrame to check
        Returns:
            SirtepStorage: Corresponding storage instance
        """

        match dt_type:
            case "matrix":
                return self.matrix_storage
            case "provision":
                return self.provision_storage
            case "response":
                return self.response_storage
            case "_":
                raise NotImplementedError(f"DataFrame type {dt_type} not implemented")

    def get_actual_cache_name(
        self, dt_type: Literal["matrix", "provision", "response"], *args
    ) -> str | None:
        """
        Retrieves the actuality date of the cached data.
        Args:
            dt_type (Literal["matrix", "provision", "response"]): Type of DataFrame to check
        Returns:
            str | None: Actuality date of the cached data or None if not found
        """

        if dt_type == "matrix":
            return self.matrix_storage.retrieve_cached_file(dt_type, ".parquet", *args)
        elif dt_type == "provision":
            return self.provision_storage.retrieve_cached_file(
                dt_type, ".parquet", *args
            )
        elif dt_type == "response":
            return self.response_storage.retrieve_cached_file(dt_type, ".pickle", *args)
        else:
            raise NotImplementedError(f"DataFrame type {dt_type} not implemented")

    def store_df(
        self, df: pd.DataFrame, df_type: Literal["matrix", "provision"], *args
    ) -> str:
        """
        Stores a DataFrame as a cached file.
        Args:
            df (pd.DataFrame): DataFrame to be cached
            df_type (Literal["matrix", "provision"]): Type of DataFrame to be cached
            *args: additional arguments for file naming (expecting request params)
        Returns:
            str: path to cached file
        """

        storage = self.get_storage(df_type)
        if df_type == "matrix":
            cacheable = CacheableDF(df)
        elif df_type == "provision":
            cacheable = CacheableDF(df)
        else:
            raise NotImplementedError(f"DataFrame type {df_type} not implemented")
        return storage.save(
            cacheable,
            df_type,
            ".parquet",
            datetime.now(),
            *args,
        )

    def read_df(
        self, df_type: Literal["matrix", "provision"], *args
    ) -> pd.DataFrame | None:

        if df_type == "matrix":
            file_name = self.matrix_storage.retrieve_cached_file(
                df_type, ".parquet", *args
            )
            if file_name:
                return CacheableDF.from_file(self.matrix_storage.cache_path, file_name)
            return None
        elif df_type == "provision":
            file_name = self.provision_storage.retrieve_cached_file(
                df_type, ".parquet", *args
            )
            if file_name:
                return CacheableDF.from_file(
                    self.provision_storage.cache_path, file_name
                )
            return None
        else:
            raise NotImplementedError(f"DataFrame type {df_type} not implemented")

    def store_response(self, response: BaseModel, *args) -> str:
        """
        Stores a Pydantic model as a cached file.
        Args:
            response (BaseModel): Pydantic model to be cached
            *args: additional arguments for file naming (expecting request params)
        Returns:
            str: path to cached file
        """

        cacheable = CacheablePydantic(response)
        return self.response_storage.save(
            cacheable,
            "response",
            ".pickle",
            datetime.now(),
            *args,
        )

    def read_response(self, *args) -> BaseModel | None:

        file_name = self.response_storage.retrieve_cached_file(
            "response", ".pickle", *args
        )
        if file_name:
            return CacheablePydantic.from_file(
                self.response_storage.cache_path, file_name
            )
        return None

    def get_storage_irrelevant_cache(self, storage: SirtepStorage) -> list[str]:
        """
        Retrieves a list of irrelevant cached files from the given storage.
        Args:
            storage (Storage): instance of Storage class from idustorage
        Returns:
            list[str]: List of irrelevant cached file names
        """

        file_list = storage.get_cache_list()
        current_date = datetime.now()
        irrelevant_files = [
            file_name
            for file_name in file_list
            if (
                current_date
                - datetime.strptime(
                    file_name.split(".")[0].split("_")[0], "%Y-%m-%d %H:%M:%S"
                )
            ).total_seconds()
            // 3600
            > int(self.config.get("CACHE_ACTUALITY"))
        ]
        return irrelevant_files

    def delete_irrelevant_cache(self) -> None:
        """
        Deletes irrelevant cached files from all storages.
        Returns:
            None
        """

        for storage in [
            i for i in self.__dict__.values() if isinstance(i, SirtepStorage)
        ]:
            irrelevant_files = self.get_storage_irrelevant_cache(storage)
            for file_name in irrelevant_files:
                try:
                    storage.delete_existing_cache(file_name)
                except Exception as e:
                    raise SystemError(
                        f"Error deleting file {file_name}: {repr(e)}"
                    ) from e

    def delete_cached_file(
        self, dt_type: Literal["matrix", "provision", "response"], *args
    ) -> str:
        """
        Deletes a specific cached file based on its type and additional arguments.
        Args:
            dt_type (Literal["matrix", "provision", "pickle"]): Type of cached file to delete
            *args: additional arguments for file naming (expecting request params)
        Returns:
            str: Name of the deleted cached file
        """

        storage = self.get_storage(dt_type)
        file_name = self.get_actual_cache_name(dt_type, *args)
        if file_name:
            storage.delete_existing_cache(file_name)
            return file_name
        raise FileNotFoundError(
            f"File {dt_type} not found in {repr(storage.cache_path)}"
        )
