import asyncio

import geopandas as gpd
import pandas as pd
from sirtep import optimize_building_schedule

from app.dependencies import urban_api_gateway
from app.gateways.urban_api_gateway import UrbanAPIGateway

from .dto import SchedulerDTO
from .modules import data_parser, matrix_builder


class SirtepService:

    def __init__(self, urban_api_gateway: UrbanAPIGateway):
        self.urban_api_gateway = urban_api_gateway

    async def collect_project_data(
        self, scenario_id: int, territory_id: int, token: str | None = None
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame]:
        tasks = [
            self.urban_api_gateway.get_scenario_living_buildings(scenario_id, token),
            self.urban_api_gateway.get_scenario_services(scenario_id, token),
            self.urban_api_gateway.get_normatives(territory_id, token),
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

    async def calculate_schedule(self, params: SchedulerDTO, token: str):
        """
        Function to calculate construction schedule
        Args:
            params (SchedulerDTO): basic scheduler data request info
            token (str): token to authenticate with urban api
        """

        scenario_data = await urban_api_gateway.get_scenario_info(params.scenario_id)
        buildings, services, normative = await self.collect_project_data(
            params.scenario_id, scenario_data["project"]["region"]["id"], token
        )
        buildings, services, normative = await self.parse_project_data(
            buildings, services, normative
        )
        binary_access_matrix = await matrix_builder(buildings, services, normative)
