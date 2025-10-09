import asyncio
from math import floor

import geopandas as gpd
import pandas as pd
from sirtep import optimize_building_schedule, optimize_provision_building_schedule
from sirtep.sirtep_dataclasses.scheduler_dataclasses import ProvisionSchedulerDataClass

from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.exceptions.http_exception_wrapper import http_exception
from app.common.parsing.sirtep_data_parser import SirtepDataParser
from app.common.storage.storage_service import StorageService

from .dto import SchedulerDTO
from .mappings import PROFILE_OBJ_PRIORITY_MAP
from .modules import matrix_builder
from .schema import (
    SchedulerOptimizationSchema,
    SchedulerProvisionSchema,
    SchedulerSimpleSchema,
)

# TODO remove to external mapping
PROVISION_PROFILES = [1, 2, 8, 10, 11, 12, 13]
PRIORITY_PROFILES = [3, 4, 5, 6, 7, 9]


class SirtepService:
    """
    Class for running sirtep functions and handling results.

    Attributes:
        urban_api_gateway (UrbanAPIClient): Urban API client
        parser (SirtepDataParser): Sirtep data parser for Urban API data
    """

    def __init__(
        self,
        urban_api_gateway: UrbanAPIClient,
        parser: SirtepDataParser,
        matrix_storage_service: StorageService,
        provision_storage_service: StorageService,
    ):
        """
        Initializes SirtepService.
        Args:
            urban_api_gateway (UrbanAPIClient): Urban API client
            parser (SirtepDataParser): Sirtep data parser
            matrix_storage_service (StorageService): Matrix storage service with matrix_cache_path
            provision_storage_service (StorageService): Provision storage service with provision_cache_path
        Returns:
            None
        """

        self.urban_api_gateway = urban_api_gateway
        self.parser = parser
        self.matrix_storage_service = matrix_storage_service
        self.provision_storage_service = provision_storage_service

    async def collect_project_data(
        self, scenario_id: int, territory_id: int, token: str | None = None
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
        tasks = [
            self.urban_api_gateway.get_scenario_living_buildings(scenario_id, token),
            self.urban_api_gateway.get_scenario_services(scenario_id, token),
            self.urban_api_gateway.get_normative(territory_id),
        ]
        return await asyncio.gather(*tasks)

    async def parse_project_data(
        self,
        buildings: gpd.GeoDataFrame,
        services: gpd.GeoDataFrame,
        normative: gpd.GeoDataFrame,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Function parses all retrieved data in appropriate format
        Args:
            buildings (gpd.GeoDataFrame): Buildings data
            services (gpd.GeoDataFrame): Services data
            normative (gpd.GeoDataFrame): Normative data
        Returns:
            tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: parsed buildings, services with normative data
        """

        tasks = [
            self.parser.async_parse_living_buildings(buildings),
            self.parser.async_parse_services(services, normative),
        ]
        return await asyncio.gather(*tasks)

    async def calculate_schedule(
        self, params: SchedulerDTO, token: str
    ) -> SchedulerOptimizationSchema:
        """
        Function to calculate construction schedule
        Args:
            params (SchedulerDTO): basic scheduler data request info
            token (str): token to authenticate with urban api
        Returns:
            SchedulerOptimizationSchema: schema with optimization results
        """

        scenario_data = await self.urban_api_gateway.get_scenario_info(
            params.scenario_id
        )
        if params.profile_id in PROVISION_PROFILES:
            buildings, services, normative = await self.collect_project_data(
                params.scenario_id, scenario_data["project"]["region"]["id"], token
            )
            buildings, services = await self.parse_project_data(
                buildings, services, normative
            )
            services = services.sjoin(
                buildings.reset_index()[["geometry", "physical_object_id"]], how="left"
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
                house_construction_period=schedule.house_construction_period.to_dict(),
                service_construction_period=schedule.service_construction_period.to_dict(),
                houses_per_period=[
                    floor(i) for i in schedule.houses_per_period.tolist()
                ],
                services_per_period=[
                    floor(i) for i in schedule.services_per_period.tolist()
                ],
                houses_area_per_period=schedule.houses_area_per_period.tolist(),
                services_area_per_period=schedule.services_area_per_period.tolist(),
                provided_per_period=schedule.provided_per_period,
                periods=schedule.periods.tolist(),
                buildings_comment=(
                    """Ни одно здание не будет построено. Увеличьте темпы строительства или количество периодов"""
                    if not [
                        i for i in schedule.house_construction_period if not pd.isna(i)
                    ]
                    else None
                ),
                services_comment=(
                    """Ни одно здание не будет построено. Увеличьте темпы строительства или количество периодов"""
                    if not [
                        i
                        for i in schedule.service_construction_period
                        if not pd.isna(i)
                    ]
                    else None
                ),
            )
            return SchedulerOptimizationSchema(provision=scheduler_dto, simple=None)
        elif params.profile_id in PRIORITY_PROFILES:
            objects = await self.urban_api_gateway.get_physical_objects(
                params.scenario_id, PROFILE_OBJ_PRIORITY_MAP[params.profile_id]
            )
            objects = await self.parser.async_parse_objects(objects, params.profile_id)
            schedule: pd.DataFrame = await asyncio.to_thread(
                optimize_building_schedule,
                objects,
                params.periods,
                params.max_area_per_period,
            )
            scheduler_dto = SchedulerSimpleSchema(
                **schedule.rename(columns={"name": "id"}).to_dict(orient="list")
            )
            return SchedulerOptimizationSchema(provision=None, simple=scheduler_dto)
        raise http_exception(
            400,
            msg="Profile id is not supported",
            _input={"profile_id": params.profile_id},
            _detail={"available_profiles": PROVISION_PROFILES + PRIORITY_PROFILES},
        )
