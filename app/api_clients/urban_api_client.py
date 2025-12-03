import asyncio

import geopandas as gpd
import pandas as pd

from app.common.api_handlers.json_api_handler import JSONAPIHandler
from app.common.exceptions.http_exception_wrapper import http_exception

LIVING_BUILDINGS_ID = 4


class UrbanAPIClient:

    def __init__(self, json_handler: JSONAPIHandler) -> None:
        self.json_handler = json_handler
        self.__name__ = "UrbanAPIClient"

    async def get_scenario_info(
        self, scenario_id: int, token: str | None = None
    ) -> dict[str, int | str | bool | None]:
        """
        Function retrieves scenario information by scenario id.
        Args:
            scenario_id (int): scenario id.
            token (str | None): authentication token, if required by the API.
        Returns:
            dict: scenario information.
        """

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        response = await self.json_handler.get(
            f"/api/v1/scenarios/{scenario_id}", headers=headers
        )
        return response

    async def get_scenario_living_buildings(
        self, scenario_id: int, token: str | None = None
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves living building information by scenario id.
        Args:
            scenario_id (int): scenario id.
            token (str | None): authentication token, if required by the API.
        Returns:
            gpd.GeoDataFrame: living buildings layer or empty gpd.GeoDataFrame.
        """

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        response = await self.json_handler.get(
            f"/api/v1/scenarios/{scenario_id}/geometries_with_all_objects",
            params={"physical_object_type_id": LIVING_BUILDINGS_ID},
            headers=headers,
        )
        if response["features"]:
            return gpd.GeoDataFrame.from_features(response, crs=4326)
        raise http_exception(
            400,
            "No living buildings found in scenario project data",
            _input={"scenario_id": scenario_id},
            _detail=None,
        )

    async def get_scenario_services(
        self,
        scenario_id: int,
        token: str | None = None,
        service_type_ids: list[int] | None = None,
    ) -> gpd.GeoDataFrame | None:
        """
        Function retrieves service information by scenario id.
        Args:
            scenario_id (int): scenario id.
            token (str | None): authentication token, if required by the API.
            service_type_ids (list[int] | None): service types ids to filter services by.
        Returns:
            gpd.GeoDataFrame | None: services layer or None.
        """

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        response = await self.json_handler.get(
            f"/api/v1/scenarios/{scenario_id}/services_with_geometry", headers=headers
        )
        if response["features"]:
            result = gpd.GeoDataFrame.from_features(response, crs=4326)
            if service_type_ids:
                result = result[result["service_type_id"].isin(service_type_ids)].copy()
            return result
        else:
            return None

    async def get_normative(
        self,
        territory_id: int,
    ) -> pd.DataFrame | None:
        """
        Function retrieves normative information by territory id.
        Args:
            territory_id (int): territory id.
        Returns:
            pd.DataFrame | None: normative layer or None.
        """

        response = await self.json_handler.get(
            f"/api/v1/territory/{territory_id}/normatives",
        )
        if response:
            return pd.DataFrame.from_records(response)
        return None

    async def get_physical_objects(
        self,
        scenario_id: int,
        physical_object_types_ids: list[int],
        token: str | None = None,
    ) -> gpd.GeoDataFrame | pd.DataFrame:
        """
        Function retrieves physical objects by their type ids.
        Args:
            scenario_id (int): scenario id.
            physical_object_types_ids (list[int]): list of physical object type ids.
            token (str | None): authentication token, if required by the API.
        Returns:
            gpd.GeoDataFrame | pd.DataFrame: physical objects layer or empty gpd.GeoDataFrame.
        """

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            tasks = [
                self.json_handler.get(
                    f"/api/v1/scenarios/{scenario_id}/physical_objects_with_geometry",
                    headers=headers,
                    params={"physical_object_type_id": obj_type_id},
                )
                for obj_type_id in physical_object_types_ids
            ]
            responses = await asyncio.gather(*tasks)
            results = [
                gpd.GeoDataFrame.from_features(response, crs=4326)
                for response in responses
                if response and response.get("features")
            ]
            return pd.concat(results)
        except Exception as e:
            raise http_exception(
                500,
                "Error during extracting physical objects from urban api",
                _input={"physical_object_types_ids": physical_object_types_ids},
                _detail={"error": repr(e)},
            ) from e

    async def get_service_types_map(self) -> dict[int, str]:
        """
        Function retrieves service types map for all services.
        Returns:
            dict: service types map.
        """

        response = await self.json_handler.get("api/v1/service_types")
        return {item["service_type_id"]: item["name"] for item in response}
