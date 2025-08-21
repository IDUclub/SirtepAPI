import geopandas as gpd
import pandas as pd

from app.common.api_handlers.json_api_handler import JSONAPIHandler

living_buildings_id = 4


class UrbanAPIGateway:

    def __init__(self, base_url: str) -> None:
        self.json_handler = JSONAPIHandler(base_url)
        self.__name__ = "UrbanAPIGateway"

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
            params={"physical_object_type_id": living_buildings_id},
            headers=headers,
        )
        if response["features"]:
            return gpd.GeoDataFrame.from_features(response, crs=4326)
        return gpd.GeoDataFrame()

    async def get_scenario_services(
        self,
        scenario_id: int,
        token: str | None = None,
        service_type_ids: list[int] | None = None,
    ) -> gpd.GeoDataFrame:
        """
        Function retrieves service information by scenario id.
        Args:
            scenario_id (int): scenario id.
            token (str | None): authentication token, if required by the API.
            service_type_ids (list[int] | None): service types ids to filter services by.
        Returns:
            gpd.GeoDataFrame: services layer or empty gpd.GeoDataFrame.
        """

        headers = {"Authorization": f"Bearer {token}"} if token else {}
        response = await self.json_handler.get(
            f"/api/v1/scenarios/{scenario_id}/services_with_geometries", headers=headers
        )
        if response["features"]:
            result = gpd.GeoDataFrame.from_features(response, crs=4326)
            if service_type_ids:
                result = result[result["service_type_ud"].isin(service_type_ids)].copy()
            return result
        else:
            return gpd.GeoDataFrame()

    async def get_normatives(
        self,
        territory_id: int,
        year: int | None = None,
    ) -> pd.DataFrame:
        """
        Function retrieves normative information by territory id.
        Args:
            territory_id (int): territory id.
            year (int): year.
        Returns:
            pd.DataFrame: normative layer or empty gpd.GeoDataFrame.
        """

        response = await self.json_handler.get(
            f"api/v1/territory/{territory_id}/normatives",
            params={"year": year},
        )
        if response:
            return pd.DataFrame.from_records(response)
        return pd.DataFrame()
