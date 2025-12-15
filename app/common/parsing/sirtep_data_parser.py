import asyncio

import geopandas as gpd
import numpy as np
import pandas as pd
from iduconfig import Config
from shapely.geometry import MultiPolygon, Polygon

from app.sirtep.mappings import PROFILE_OBJ_PRIORITY_MAP


class SirtepDataParser:
    """
    Class for data parsing from urban api in appropriate format for sirtep lib use.

    Attributes:
        config (Config)
    """

    def __init__(self, config: Config):
        """
        Initialise the data parser.
        Args:
            config (Config): The configuration object to get env variables.
        Returns:
            None
        """

        self.config = config

    def parse_living_buildings(
        self, living_buildings: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw buildings gdf. Pay attention, layer should be reprojected to local crs
        before input.
        Args:
            living_buildings (gpd.GeoDataFrame): raw buildings gdf
        Returns:
            gpd.GeoDataFrame: parsed living buildings gdf in input crs
        """

        living_buildings["physical_object_id"] = living_buildings[
            "physical_objects"
        ].apply(lambda x: x[0]["physical_object_id"])
        living_buildings_gdf = living_buildings.set_index(
            "physical_object_id", drop=True
        )
        unique_object_keys = set(
            np.hstack(
                living_buildings["physical_objects"].apply(
                    lambda x: (
                        list(x[0]["building"].keys()) if x[0]["building"] else None
                    )
                ),
            )
        )
        if "floors" not in unique_object_keys:
            living_buildings_gdf["floors"] = int(self.config.get("DEFAULT_FLOORS"))
        else:
            living_buildings_gdf["floors"] = living_buildings_gdf[
                "physical_objects"
            ].apply(lambda x: x[0]["building"].get("floors"))
            if len(living_buildings_gdf.dropna(subset=["floors"])) < 1:
                living_buildings_gdf["floors"] = int(self.config.get("DEFAULT_FLOORS"))
            else:
                living_buildings_gdf["floors"] = living_buildings_gdf["floors"].fillna(
                    living_buildings_gdf["floors"].mean()
                )
        living_buildings_gdf["living_area"] = (
            living_buildings_gdf.to_crs(living_buildings_gdf.estimate_utm_crs()).area
            * living_buildings_gdf["floors"]
            * float(self.config.get("LIVING_AREA_COEF"))
        )
        living_buildings_gdf["population"] = living_buildings_gdf["living_area"] / int(
            self.config.get("METRES_PER_HUMAN")
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

    def parse_services(
        self, services: gpd.GeoDataFrame, normative: pd.DataFrame
    ) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw services gdf. Pay attention, layer should be reprojected to local crs
        before input.
        Args:
            services (gpd.GeoDataFrame): raw services gdf
            normative (pd.DataFrame): normative data df
        Returns:
            gpd.GeoDataFrame: parsed services gdf
        """

        services_gdf = services.copy()
        services_gdf["service_area"] = services_gdf.to_crs(
            services_gdf.estimate_utm_crs()
        ).area.copy()
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
        services_gdf["service_type_id"] = services_gdf["service_type"].apply(
            lambda x: x["service_type_id"]
        )
        services_gdf = pd.merge(
            services_gdf,
            normative[
                [
                    "service_type_id",
                    "services_capacity_per_1000_normative",
                    "time_availability_minutes",
                ]
            ],
            left_on="service_type_id",
            right_on="service_type_id",
            how="left",
        )
        services_gdf["radius_availability_meters"] = (
            services_gdf["time_availability_minutes"]
            * int(self.config.get("MEDIUM_SPEED"))
            * 1000
            / 60
        )
        services_gdf["capacity"] = (
            services_gdf["capacity"]
            * 1000
            / services_gdf["services_capacity_per_1000_normative"]
        )
        services_gdf["geometry"] = services.representative_point()
        return services_gdf.set_index("service_id", drop=True)

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

    def parse_objects(
        self, objects: gpd.GeoDataFrame, profile_id: int
    ) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw objects gdf. Pay attention, layer should be reprojected to local crs
        before input.
        Args:
            objects (gpd.GeoDataFrame): raw objects gdf
            profile_id (int): profile id
        Returns:
            gpd.GeoDataFrame: parsed objects gdf
        """

        objects_gdf = objects.set_index("physical_object_id", drop=True)
        objects_gdf["physical_object_type_id"] = objects_gdf[
            "physical_object_type"
        ].apply(lambda x: x["physical_object_type_id"])
        objects_gdf["priority"] = objects_gdf["physical_object_type_id"].map(
            lambda x: PROFILE_OBJ_PRIORITY_MAP[profile_id].index(x)
        )
        objects_gdf.to_crs(objects.estimate_utm_crs(), inplace=True)
        objects_gdf["geometry"] = objects_gdf["geometry"].apply(
            lambda geom: (
                geom
                if geom.geom_type in [Polygon, MultiPolygon]
                else geom.buffer(int(self.config.get("NON_POLY_OBJECTS_BUFFER")))
            )
        )
        objects_gdf["area"] = objects_gdf.area.copy()
        return objects_gdf.to_crs(4326)

    async def async_parse_objects(
        self, objects: gpd.GeoDataFrame, profile_id: int
    ) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw objects asynchronously
        Args:
            objects (gpd.GeoDataFrame): raw objects gdf
            profile_id (int): profile_id for priority mapping
        Returns:
            gpd.GeoDataFrame: parsed objects gdf
        """

        return await asyncio.to_thread(self.parse_objects, objects, profile_id)
