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

    def to_file(
        self, path: Path, name: str, ext: str, date: str, separator: str, *args
    ) -> Path:
        """
        Function caches DataFrame to parquet file.
        Args:
            path (Path): path to cache fileб
            name (str): name of cached file
            ext (str): extension of cached file
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
            self.df.to_parquet(file_path)
            return path
        except Exception as e:
            raise SystemError(
                f"Error caching DataFrame with cache_path {repr(path)} with error {repr(e)}"
            ) from e

    @staticmethod
    def from_file(path: Path, name: str) -> pd.DataFrame:
        """
        Function reads DataFrame from parquet file.
        Args:
            path (Path): path to cache file
            name (str): name of cached file
        Returns:
            pd.DataFrame: DataFrame read from cache file
        Raises:
            SystemError: if error occurs during reading
        """

        try:
            return pd.read_parquet(path / name)
        except Exception as e:
            raise SystemError(f"Error reading parquet file with path {path}") from e
