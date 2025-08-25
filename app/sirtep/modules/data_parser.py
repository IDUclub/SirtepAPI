import asyncio

import geopandas as gpd
import numpy as np
import pandas as pd

DEFAULT_FLOORS = 3
LIVING_AREA_COEF = 0.8
METRES_PER_HUMAN = 20
MEDIUM_SPEED = 40  # km per hour


class DataParser:

    @staticmethod
    def parse_living_buildings(living_buildings: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw buildings gdf. Pay attention, layer should be reprojected to local crs
        before input.
        Args:
            living_buildings (gpd.GeoDataFrame): raw buildings gdf
        Returns:
            gpd.GeoDataFrame: parsed living buildings gdf in input crs
        """

        living_buildings_gdf = living_buildings.set_index(
            "physical_object_id", drop=True
        )
        unique_object_keys = list(
            set(
                np.reshape(
                    living_buildings["physical_objects"].apply(
                        lambda x: x["building"].keys()
                    ),
                    -1,
                )
            )
        )
        if "floors" not in unique_object_keys:
            living_buildings_gdf["floors"] = 3
        else:
            living_buildings_gdf["floors"] = living_buildings_gdf[
                "physical_objects"
            ].apply(lambda x: x["floors"])
            living_buildings_gdf["floors"] = living_buildings_gdf["floors"].fillna(
                living_buildings_gdf["floors"].mean()
            )
        living_buildings_gdf["living_area"] = (
            living_buildings_gdf.area
            * living_buildings_gdf["floors"]
            * LIVING_AREA_COEF
        )
        living_buildings_gdf["population"] = (
            living_buildings_gdf["living_area"] / METRES_PER_HUMAN
        )
        return living_buildings_gdf

    async def async_parse_living_buildings(
        self, living_buildings: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw buildings gdf asynchronously
        Args:
            living_buildings (gpd.GeoDataFrame): raw buildings gdf
        Returns:
            gpd.GeoDataFrame: parsed living buildings gdf
        """

        return await asyncio.to_thread(self.parse_living_buildings, living_buildings)

    @staticmethod
    def parse_services(
        services: gpd.GeoDataFrame, normative: pd.DataFrame
    ) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
        """
        Function parses all data from raw services gdf. Pay attention, layer should be reprojected to local crs
        before input.
        Args:
            services (gpd.GeoDataFrame): raw services gdf
            normative (pd.DataFrame): normative data df
        Returns:
            gpd.GeoDataFrame: parsed services gdf
        """

        services_gdf = services.set_index("service_id", drop=True)
        services_gdf["service_area"] = services_gdf.area.copy()
        services_gdf["weight"] = services_gdf["service_type"].apply(
            lambda x: (
                x["properties"]["weight_value"]
                if "weight_value" in x["properties"].keys()
                else None
            )
        )
        services_gdf.dropna(subset=["weight"], inplace=True)
        services_gdf["capacity"] = services_gdf["service_type"].apply(
            lambda x: x["capacity_modeled"]
        )
        normative["service_type_id"] = normative["service_type"].apply(
            lambda x: x["id"]
        )
        services_gdf["radius_availability_meters"] = (
            services_gdf["time_availability_minutes"] * MEDIUM_SPEED * 1000 / 60
        )
        services_gdf["capacity"] = (
            services_gdf["capacity"]
            * 1000
            / services_gdf["services_capacity_per_1000_normative"]
        )
        return services_gdf, normative

    async def async_parse_services(
        self, services: gpd.GeoDataFrame, normative: pd.DataFrame
    ) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw services asynchronously
        Args:
            services (gpd.GeoDataFrame): raw services gdf
            normative (pd.DataFrame): normative data df
        Returns:
            gpd.GeoDataFrame: parsed services gdf
        """

        return await asyncio.to_thread(self.parse_services, services, normative)


data_parser = DataParser()
