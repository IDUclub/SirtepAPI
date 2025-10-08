from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .caching_service import CachingService


class ParquetCachingService(CachingService):

    def __init__(self, cache_path: Path) -> None:
        """
        Function initialize caching service class
        Args:
            cache_path: path to cache file
        Returns:
            None
        """

        super().__init__(cache_path)
        self.CACHING_PARAMS = [
            "scenario_id",
            "profile_id",
            "periods",
            "max_area_per_period",
            "scenario_update_date",
            "cache_creation_date",
        ]

    def create_cache_name(
        self,
        scenario_id: int,
        profile_id: int,
        periods: int,
        max_area_per_period: int,
        creation_date: date,
        scenario_update_date: datetime,
    ) -> Path:
        """
        Function caches dataframe to feather. Dates parses in "YYYY-MM-DD" format.
        Args:
            scenario_id (int): scenario id
            profile_id (int): profile id
            periods (int): number of periods
            max_area_per_period (int): max area per period
            creation_date (datetime): cache creation date
            scenario_update_date (datetime): scenario last update time
        Returns:
            str: path to cached file
        """

        creation_date = creation_date.strftime("%Y-%m-%d")
        scenario_update_date = scenario_update_date.strftime("%Y-%m-%d-%H-%M-%S")
        cache_name = f"{scenario_id}_{profile_id}_{periods}_{max_area_per_period}_{creation_date}_{scenario_update_date}.parquet"
        return self.caching_path.joinpath(cache_name)

    def parse_name_from_cache(self, cache_path: Path) -> dict:

        name, extension = cache_path.name.split(".")[:]
        parsed_params = {
            self.CACHING_PARAMS[i]: name.split("_")[i]
            for i in range(len(self.CACHING_PARAMS))
        }
        parsed_params["extension"] = extension
        return parsed_params

    def cache_df(
        self, df: pd.DataFrame, cache_variables: list[int | str | datetime | date]
    ) -> Path:
        """
        Function caches dataframe to feather. Dates parses in "YYYY-MM-DD" format.
        Args:
            df (pd.DataFrame): dataframe to cache
            cache_variables (list[int | str | datetime | date]): variables to create cache name. Order: scenario_id, profile_id, periods, max_area_per_period, scenario_update_date
        Returns:
            Path: path to cached file
        """

        cache_path = self.create_cache_name(*cache_variables)
        try:
            df.to_parquet(cache_path)
        except Exception as e:
            raise Exception(
                f"Error caching DataFrame with cache_path {cache_path}. Caching params: {self.parse_name_from_cache(cache_path)}"
            ) from e
        return cache_path
