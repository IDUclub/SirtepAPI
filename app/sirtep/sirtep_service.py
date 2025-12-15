import asyncio
from datetime import datetime
from math import floor

import geopandas as gpd
import pandas as pd
from loguru import logger
from sirtep import optimize_building_schedule, optimize_provision_building_schedule
from sirtep.sirtep_dataclasses.scheduler_dataclasses import ProvisionSchedulerDataClass

from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.exceptions.http_exception_wrapper import http_exception
from app.common.parsing.sirtep_data_parser import SirtepDataParser
from app.common.storage.storage_service import StorageService

from ..common.tasks.task_service import TaskService
from .dto import ProvisionDTO, SchedulerDTO
from .mappings import PROFILE_OBJ_PRIORITY_MAP
from .modules import matrix_builder
from .schema import (
    ProvisionInProgressSchema,
    ProvisionSchema,
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
        storage_service (StorageService): storage service for cacheable objects
        task_service (TaskService): task service
    """

    def __init__(
        self,
        urban_api_gateway: UrbanAPIClient,
        parser: SirtepDataParser,
        storage_service: StorageService,
        task_service: TaskService,
    ):
        """
        Initializes SirtepService.
        Args:
            urban_api_gateway (UrbanAPIClient): Urban API client
            parser (SirtepDataParser): Sirtep data parser
            storage_service (StorageService): storage service for cacheable objects
            task_service (TaskService): task service
        Returns:
            None
        """

        self.urban_api_gateway = urban_api_gateway
        self.parser = parser
        self.storage_service = storage_service
        self.task_service = task_service

    @staticmethod
    async def get_available_profiles() -> list[int]:

        return sorted(list(set(PROVISION_PROFILES + PRIORITY_PROFILES)))

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
        normative: pd.DataFrame,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Function parses all retrieved data in appropriate format
        Args:
            buildings (gpd.GeoDataFrame): Buildings data
            services (gpd.GeoDataFrame): Services data
            normative (gpd.GeoDataFrame): Normative data
        Returns:
            [gpd.GeoDataFrame, gpd.GeoDataFrame]: parsed buildings, services with normative data
        """

        tasks = [
            self.parser.async_parse_living_buildings(buildings),
            self.parser.async_parse_services(services, normative),
        ]
        return tuple(await asyncio.gather(*tasks))

    async def calculate_provision(
        self,
        buildings: gpd.GeoDataFrame,
        services: gpd.GeoDataFrame,
        matrix: pd.DataFrame,
        buildings_built_periods: dict[int, int | None],
        services_built_periods: dict[int, int | None],
        num_periods: int,
        service_types: list[int],
        task_id: str,
        *args,
    ) -> None:
        """
        Function calculates and caches provision for all services in requested optimization.
        Should be run in background.
        Args:
            buildings (gpd.GeoDataFrame): Buildings data
            services (gpd.GeoDataFrame): Services data
            matrix (pd.DataFrame): Binary access matrix
            buildings_built_periods (list[int]): List of periods when buildings are built
            services_built_periods (list[int]): List of periods when services are built
            num_periods (int): Number of periods in optimization
            service_types (list[int]): Service types ids list
            task_id (str): Task id to update task status
            *args: request arguments for cache file naming
        Returns:
            None
        """

        result_df = pd.DataFrame(columns=service_types, index=range(num_periods))
        total_to_process = len(buildings_built_periods) * len(service_types)
        processed = 0
        self.task_service.set_task_status(task_id, "pending")
        b_df, s_df = [
            pd.DataFrame(i.items(), columns=["id", "period"]).dropna(subset=["period"])
            for i in (buildings_built_periods, services_built_periods)
        ]  # DataFrames with built periods and objects ids (index, id, period)

        # iterating over all periods
        for period in range(num_periods):
            # data for current period (buiuldings, services, matrix)
            b_period_ids, s_period_ids = [
                df[df["period"] <= period]["id"].to_list() for df in (b_df, s_df)
            ]
            period_matrix = matrix.loc[
                matrix.index.isin(b_period_ids), matrix.columns.isin(s_period_ids)
            ]
            period_buildings = buildings[buildings.index.isin(b_period_ids)]
            period_services = services[services.index.isin(s_period_ids)]

            # iterating over all service types
            for service_type_id in service_types:

                if not service_type_id in period_services["service_type_id"].unique():
                    result_df.loc[period, service_type_id] = (
                        0  # provision equals to 0 if no services of this type are built
                    )
                else:

                    # data for current service type
                    current_type_services = period_services[
                        period_services["service_type_id"] == service_type_id
                    ]
                    available_matrix = period_matrix.loc[
                        period_buildings.index.intersection(period_matrix.index),
                        current_type_services.index.intersection(period_matrix.columns),
                    ]
                    available_matrix = (
                        available_matrix[available_matrix > 0]
                        .dropna(how="all", axis=0)
                        .dropna(how="all", axis=1)
                    )

                    if available_matrix.empty:
                        result_df.loc[period, service_type_id] = (
                            0  # if no buildings have access to services of this type provision equals to 0
                        )
                    else:
                        demand = period_buildings.loc[available_matrix.index,][
                            "population"
                        ].sum()
                        capacity = current_type_services.loc[available_matrix.columns,][
                            "capacity"
                        ].sum()
                        result_df.loc[period, service_type_id] = int(
                            round(capacity / demand * 100, 2)
                        )

                # updating task progress
                processed += 1
                self.task_service.set_task_progress(
                    task_id,
                    round(processed / total_to_process * 100),
                )

        self.storage_service.store_df(result_df, "provision", *args)
        self.task_service.set_task_status(task_id, "complete d")

    async def check_generation(self, last_scenario_update: str, *args) -> str:
        """
        Function checks if the cached response is up to date with the last scenario update
        Args:
            last_scenario_update (str): last scenario update date
            *args: request arguments for cache file naming
        Returns:
            str: True if the cached response is up to date, False otherwise
        Raises:
            Exception: If there is an error while checking the cache
        """

        try:
            cache_name = self.storage_service.get_actual_cache_name("response", *args)
            if cache_name:
                if last_scenario_update == cache_name.split("_")[-1]:
                    return cache_name
            return ""
        except Exception as e:
            logger.exception(f"Exception while checking generation cache: {repr(e)}")
            raise

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
            params.scenario_id, token=token
        )
        actual_scenario_update = datetime.strptime(
            scenario_data["updated_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(microsecond=0)
        cache_args = params.request_params_as_list()
        if cache_name := self.storage_service.get_actual_cache_name(
            "response", *cache_args
        ):
            last_scenario_update = datetime.strptime(
                cache_name.split(".")[0].split("_")[-1], "%Y-%m-%d-%H-%M-%S"
            )
            if actual_scenario_update == last_scenario_update:
                response = self.storage_service.read_response(*cache_args)
                return SchedulerOptimizationSchema(provision=response, simple=None)

        cache_args.append(actual_scenario_update.strftime("%Y-%m-%d-%H-%M-%S"))

        if params.profile_id in PROVISION_PROFILES:
            buildings, services, normative = await self.collect_project_data(
                params.scenario_id, int(scenario_data["project"]["region"]["id"]), token
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

            self.storage_service.store_df(binary_access_matrix, "matrix", *cache_args)
            self.storage_service.store_response(scheduler_dto, *cache_args)
            task_id_params = [str(i) for i in cache_args]
            self.task_service.create_task("tep", *task_id_params)
            asyncio.create_task(
                self.calculate_provision(
                    buildings,
                    services,
                    binary_access_matrix,
                    scheduler_dto.house_construction_period,
                    scheduler_dto.service_construction_period,
                    params.periods,
                    services["service_type_id"].unique().tolist(),
                    "_".join(task_id_params),
                    *cache_args,
                )
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

    async def get_provision_for_request(
        self, params: ProvisionDTO, token: str
    ) -> ProvisionSchema | ProvisionInProgressSchema:
        """
        Function retrieves provision DataFrame for the given request parameters or returns info.
        Args:
            params (ProvisionDTO): Request parameters
            token (str): Request token
        Returns:
            ProvisionSchema | ProvisionInProgressSchema: Provision data if available, else status info
        Raises:
            HTTP_Exception 400: If the optimization was never started
        """

        scenario_data = await self.urban_api_gateway.get_scenario_info(
            params.scenario_id, token
        )
        cache_args = params.request_params_as_list()
        task_id = "_".join([str(i) for i in cache_args])
        actual_scenario_update = datetime.strptime(
            scenario_data["updated_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(microsecond=0)
        cache_args = params.request_params_as_list()
        if cache_name := self.storage_service.get_actual_cache_name(
            "response", *cache_args
        ):
            last_scenario_update = datetime.strptime(
                cache_name.split(".")[0].split("_")[-1], "%Y-%m-%d-%H-%M-%S"
            )
            if actual_scenario_update != last_scenario_update:
                # TODO run new optimization with provided params
                raise http_exception(
                    400,
                    msg=f"Оптимизация с параметрами {params.request_params_as_list()} устарела, сценарные данные были обновлены. Запустите оптимизацию заново",
                )
            provision_df = self.storage_service.read_df("provision", *cache_args)
            if isinstance(provision_df, pd.DataFrame):
                service_id_name_map = (
                    await self.urban_api_gateway.get_service_types_map()
                )
                provision_df.rename(columns=service_id_name_map, inplace=True)
                return ProvisionSchema(
                    periods=[i for i in range(len(provision_df))],
                    provision=provision_df.to_dict(orient="records"),
                )
            else:
                raise http_exception(
                    500,
                    msg=f"Кэш с результатами оптимизации с параметрами {params.request_params_as_list()} повреждён. Попробуйте запустить оптимизацию заново",
                    _input={"Параметры запроса": params.request_params_as_dict()},
                    _detail=None,
                )
        if task := self.task_service.get_task(task_id):
            if task.status == "pending":
                return ProvisionInProgressSchema(
                    status=task.status,
                    progress=task.task_progress,
                    message="Ведётся расчёт ТЭПов",
                )
            elif task.status == "in_queue":
                return ProvisionInProgressSchema(
                    status=task.status,
                    progress=task.task_progress,
                    message="Задача в очереди на расчёт ТЭПов",
                )
            elif task.status == "error":
                return ProvisionInProgressSchema(
                    status=task.status,
                    progress=task.task_progress,
                    message="Произошла ошибка при расчёте ТЭПов. Попробуйте запустить оптимизацию заново или обратитесь в поддержку",
                )
            else:
                k_e = KeyError(f"Task {task_id} has unexpected status {task.status}")
                logger.error(repr(k_e))
                raise k_e
        raise http_exception(
            400,
            msg=f"Оптимизация с параметрами {params.request_params_as_list()} не была запущена. Сначала запустите оптимизацию",
        )
