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

    def write(self, path: Path) -> Path:
        """
        Function caches Pydantic model to pickle file.
        Args:
            path (Path): path to cache file
        Returns:
            Path: path to cached file
        Raises:
            SystemError: if error occurs during caching
        """

        try:
            with open(path, "wb") as f:
                pickle.dump(self.model, f)
            return path
        except Exception as e:
            raise SystemError(
                f"Error caching Pydantic model with cache_path {repr(path)}"
            ) from e

    @staticmethod
    def read(path: Path) -> BaseModel:
        """
        Function reads Pydantic model from pickle file.
        Args:
            path (Path): path to cache
        Returns:
            BaseModel: Pydantic model read from cache file
        Raises:
            SystemError: if error occurs during reading
        """

        try:
            with open(path, "rb") as f:
                return pickle.load(f)
        except Exception as e:
            raise SystemError(
                f"Error reading Pydantic model from file with path {path}"
            ) from e
