import asyncio

import geopandas as gpd
import pandas as pd
from sirtep import optimize_building_schedule, optimize_provision_building_schedule
from sirtep.sirtep_dataclasses.scheduler_dataclasses import ProvisionSchedulerDataClass

from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.exceptions.http_exception_wrapper import http_exception
from app.dependencies import urban_api_gateway

from .dto import SchedulerDTO
from .mappings import PROFILE_OBJ_PRIORITY_MAP
from .modules import data_parser, matrix_builder
from .schema import (
    SchedulerOptimizaionSchema,
    SchedulerProvisionSchema,
    SchedulerSimpleSchema,
)

# TODO remove to external mapping
PROVISION_PROFILES = [1, 2, 8, 10, 11, 12, 13]
PRIORITY_PROFILES = [3, 4, 5, 6, 7, 9]


class SirtepService:

    def __init__(self, urban_api_gateway: UrbanAPIClient):
        self.urban_api_gateway = urban_api_gateway

    async def collect_project_data(
        self, scenario_id: int, territory_id: int, token: str | None = None
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
        tasks = [
            self.urban_api_gateway.get_scenario_living_buildings(scenario_id, token),
            self.urban_api_gateway.get_scenario_services(scenario_id, token),
            self.urban_api_gateway.get_normative(territory_id, token),
        ]
        return await asyncio.gather(*tasks)

    async def parse_project_data(
        self,
        buildings: gpd.GeoDataFrame,
        services: gpd.GeoDataFrame,
        normative: gpd.GeoDataFrame,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
        """
        Function parses all retrieved data in appropriate format
        Args:
            buildings (gpd.GeoDataFrame): Buildings data
            services (gpd.GeoDataFrame): Services data
            normative (gpd.GeoDataFrame): Normative data
        Returns:
            tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]: parsed buildings, services and normative data
        """

        tasks = [
            data_parser.async_parse_living_buildings(buildings),
            data_parser.async_parse_services(services, normative),
        ]
        return await asyncio.gather(*tasks)

    async def calculate_schedule(
        self, params: SchedulerDTO, token: str
    ) -> SchedulerOptimizaionSchema:
        """
        Function to calculate construction schedule
        Args:
            params (SchedulerDTO): basic scheduler data request info
            token (str): token to authenticate with urban api
        Returns:
            SchedulerOptimizaionSchema: schema with optimization results
        """

        scenario_data = await urban_api_gateway.get_scenario_info(params.scenario_id)
        if params.profile_id in PROVISION_PROFILES:
            buildings, services, normative = await self.collect_project_data(
                params.scenario_id, scenario_data["project"]["region"]["id"], token
            )
            buildings, services = await self.parse_project_data(
                buildings, services, normative
            )
            binary_access_matrix = await matrix_builder.async_build_distance_matrix(
                buildings, services
            )
            schedule: ProvisionSchedulerDataClass = await asyncio.to_thread(
                optimize_provision_building_schedule,
                buildings,
                services,
                binary_access_matrix,
                params.max_area_per_period,
                params.periods,
                verbose=False,
            )
            scheduler_dto = SchedulerProvisionSchema(
                x=schedule.x_val.tolist(),
                y=schedule.y_val.tolist(),
                house_construction_period=schedule.house_construction_period.to_dict(),
                service_construction_period=schedule.service_construction_period.to_dict(),
                houses_per_period=schedule.houses_per_period.tolist(),
                services_per_period=schedule.services_per_period.tolist(),
                houses_area_per_period=schedule.houses_area_per_period.tolist(),
                services_area_per_period=schedule.services_area_per_period.tolist(),
                provided_per_period=schedule.provided_per_period,
                periods=schedule.periods.tolist(),
            )
            return SchedulerOptimizaionSchema(provision=scheduler_dto, simple=None)
        elif params.profile_id in PRIORITY_PROFILES:
            objects = await urban_api_gateway.get_physical_objects(
                params.scenario_id, PROFILE_OBJ_PRIORITY_MAP[params.profile_id]
            )
            objects = await data_parser.async_parse_objects(objects, params.profile_id)
            schedule: pd.DataFrame = await asyncio.to_thread(
                optimize_building_schedule,
                objects,
                params.periods,
                params.max_area_per_period,
            )
            scheduler_dto = SchedulerSimpleSchema(
                **schedule.rename(columns={"name": "id"}).to_dict(orient="list")
            )
            return SchedulerOptimizaionSchema(provision=None, simple=scheduler_dto)
        raise http_exception(
            400,
            msg="Profile id is not supported",
            _input={"profile_id": params.profile_id},
            _detail={"available_profiles": PROVISION_PROFILES + PRIORITY_PROFILES},
        )


sirtep_service = SirtepService(urban_api_gateway)
