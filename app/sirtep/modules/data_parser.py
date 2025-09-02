import asyncio

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon

from app.dependencies import config
from app.sirtep.mappings import PROFILE_OBJ_PRIORITY_MAP

DEFAULT_FLOORS = int(config.get("DEFAULT_FLOORS"))
LIVING_AREA_COEF = float(config.get("LIVING_AREA_COEF"))
METRES_PER_HUMAN = int(config.get("METRES_PER_HUMAN"))
MEDIUM_SPEED = int(config.get("MEDIUM_SPEED"))  # km per hour
NON_POLY_OBJECTS_BUFFER = int(config.get("NON_POLY_OBJECTS_BUFFER"))  # meters


class DataParser:
    """
    Class for data parsing from urban api in appropriate format for sirtep lib use.

    Attributes:
        default_floors (int): The default floor number of the project.
        living_area_coef (float): The maximum area of the project to be considered. 0.0 < living_area_coef < 1.0.
        metres_per_human (int): The maximum area of the project to be considered.
        medium_speed (int): The medium speed of the project to be considered.
        non_poly_objects_buffer (int): The maximum area of the project to be considered (e.g. roads)
    """

    def __init__(
        self,
        default_floors: int,
        living_area_coef: float,
        metres_per_human: int,
        medium_speed: int,
        non_poly_objects_buffer: int,
    ):
        """
        Initialise the data parser.
        Args:
            default_floors (int): The default floor number of the project.
            living_area_coef (float): The maximum area of the project to be considered. 0.0 < living_area_coef < 1.0.
            metres_per_human (int): The maximum area of the project to be considered.
            medium_speed (int): The medium speed of the project to be considered.
            non_poly_objects_buffer (int): The maximum area of the project to be considered (e.g. roads)
        Returns:
            None
        """

        self.default_floors = default_floors
        self.living_area_coef = living_area_coef
        self.metres_per_human = metres_per_human
        self.medium_speed = medium_speed
        self.non_poly_objects_buffer = non_poly_objects_buffer

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

        living_buildings["physical_object_id"] = living_buildings[
            "physical_objects"
        ].apply(lambda x: x[0]["physical_object_id"])
        living_buildings_gdf = living_buildings.set_index(
            "physical_object_id", drop=True
        )
        unique_object_keys = set(
            np.reshape(
                living_buildings["physical_objects"].apply(
                    lambda x: x[0]["building"].keys() if x[0]["building"] else None
                ),
                -1,
            )
        )
        if "floors" not in unique_object_keys:
            living_buildings_gdf["floors"] = DEFAULT_FLOORS
        else:
            living_buildings_gdf["floors"] = living_buildings_gdf[
                "physical_objects"
            ].apply(lambda x: x["floors"])
            living_buildings_gdf["floors"] = living_buildings_gdf["floors"].fillna(
                living_buildings_gdf["floors"].mean()
            )
        living_buildings_gdf["living_area"] = (
            living_buildings_gdf.to_crs(living_buildings_gdf.estimate_utm_crs()).area
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

        services_gdf = services.set_index("service_id", drop=True)
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
            services_gdf["time_availability_minutes"] * MEDIUM_SPEED * 1000 / 60
        )
        services_gdf["capacity"] = (
            services_gdf["capacity"]
            * 1000
            / services_gdf["services_capacity_per_1000_normative"]
        )
        return services_gdf

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

    @staticmethod
    def parse_objects(objects: gpd.GeoDataFrame, profile_id: int) -> gpd.GeoDataFrame:
        """
        Function parses all data from raw objects gdf. Pay attention, layer should be reprojected to local crs
        before input.
        Args:
            objects (gpd.GeoDataFrame): raw objects gdf
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
                else geom.buffer(NON_POLY_OBJECTS_BUFFER)
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


data_parser = DataParser(
    int(config.get("DEFAULT_FLOORS")),
    float(config.get("LIVING_AREA_COEF")),
    int(config.get("METRES_PER_HUMAN")),
    int(config.get("MEDIUM_SPEED")),
    int(config.get("NON_POLY_OBJECTS_BUFFER")),
)
