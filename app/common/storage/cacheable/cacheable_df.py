from pathlib import Path

import pandas as pd
from idustorage import Cacheable


class CacheableDF(Cacheable):
    """
    Class for caching pandas DataFrames
    Attributes:
        df (pd.DataFrame): pandas DataFrame to be cached
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initializes CacheableDF with a pandas DataFrame.
        Args:
            df (pd.DataFrame): DataFrame to be cached
        Raises:
            TypeError: if df is not a pandas DataFrame
        """

        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Object {df} is not a pandas DataFrame")
        self.df = df

    def write(self, path: Path) -> Path:
        """
        Function caches DataFrame to parquet file.
        Args:
            path (Path): path to cache file
        Returns:
            Path: path to cached file
        Raises:
            SystemError: if error occurs during caching
        """

        try:
            self.df.to_parquet(path)
            return path
        except Exception as e:
            raise SystemError(
                f"Error caching DataFrame with cache_path {repr(path)}"
            ) from e

    @staticmethod
    def read(path: Path) -> pd.DataFrame:
        """
        Function reads DataFrame from parquet file.
        Args:
            path (Path): path to cache file
        Returns:
            pd.DataFrame: DataFrame read from cache file
        Raises:
            SystemError: if error occurs during reading
        """

        try:
            return pd.read_parquet(path)
        except Exception as e:
            raise SystemError(f"Error reading parquet file with path {path}") from e
