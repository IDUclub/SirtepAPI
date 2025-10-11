import pickle
from pathlib import Path

from idustorage import Cacheable
from pydantic import BaseModel


class CacheablePydantic(Cacheable):
    """
    Class for caching Pydantic models
    Attributes:
        model (BaseModel): Pydantic model to be cached
    Raises:
        TypeError: if model is not a Pydantic model
    """

    def __init__(self, model: BaseModel):
        """
        Initializes CacheablePydantic with a Pydantic model.
        Args:
            model (BaseModel): Pydantic model to be cached
        Raises:
            TypeError: if model is not a Pydantic model
        """

        if not isinstance(model, BaseModel):
            raise TypeError(f"Model {model} is not a Pydantic model")
        self.model = model

    def to_file(
        self, path: Path, name: str, ext: str, date: str, separator: str, *args
    ) -> Path:
        """
        Function caches Pydantic model to pickle file.
        Args:
            path (Path): path to cache file
            name (str): name of cached file
            ext (str): file extension
            date (str): date of cached file
            separator (str): separator between cached file
            *args: additional arguments
        Returns:
            Path: path to cached file
        Raises:
            SystemError: if error occurs during caching
        """

        file_name_list = [date, name, *[str(arg) for arg in args]]
        file_path = path / f"{separator.join(file_name_list)}{ext}"

        try:
            with open(file_path, "wb") as f:
                pickle.dump(self.model, f)
            return path
        except Exception as e:
            raise SystemError(
                f"Error caching Pydantic model with cache_path {repr(path)}"
            ) from e

    @staticmethod
    def from_file(path: Path, name: str) -> BaseModel:
        """
        Function reads Pydantic model from pickle file.
        Args:
            path (Path): path to cache
            name (str): name of cached file
        Returns:
            BaseModel: Pydantic model read from cache file
        Raises:
            SystemError: if error occurs during reading
        """

        try:
            with open(path / name, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            raise SystemError(
                f"Error reading Pydantic model from file with path {path}"
            ) from e
