import asyncio

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import KDTree


class MatrixBuilder:

    @staticmethod
    def calculate_availability_matrix(
        buildings: gpd.GeoDataFrame,
        services: gpd.GeoDataFrame,
        max_distance: float,
    ) -> pd.DataFrame:
        """
        Calculated availability matrix with walk simulation
        Args:
            buildings (gpd.GeoDataFrame): Building geometries
            services (gpd.GeoDataFrame): Service geometries
            max_distance (float): Maximum distance between buildings and services
        Returns:
            pd.DataFrame: Availability matrix with distance in minutes
        """

        local_crs = buildings.estimate_utm_crs()
        buildings = buildings.to_crs(local_crs).set_index(buildings.index, drop=True)
        services = services.to_crs(local_crs).set_index(services.index, drop=True)
        buildings_points = [
            geometry.coords[0] for geometry in buildings.geometry.centroid
        ]
        services_points = [
            geometry.coords[0] for geometry in services.geometry.centroid
        ]
        buildings_kd_tree = KDTree(buildings_points)
        services_kd_tree = KDTree(services_points)
        distances = buildings_kd_tree.sparse_distance_matrix(
            other=services_kd_tree, max_distance=max_distance
        )
        matrix = pd.DataFrame.sparse.from_spmatrix(
            distances, index=buildings.index, columns=services.index
        )
        matrix = matrix.sparse.to_dense()
        matrix.replace(0.0, np.nan, inplace=True)
        return matrix

    @staticmethod
    def filter_matrix(access_matrix: pd.DataFrame, services: gpd.GeoDataFrame):
        """
        Function filters matrix by each normative replacing values with 1 and 0 for binary representation of available
        services
        Args:
            access_matrix (pd.DataFrame): Matrix
            services (gpd.GeoDataFrame): Services layer with geometries and normative data
        Returns:
            pd.DataFrame: Filtered matrix
        """

        for service_type_id in services["service_type_id"].unique():
            current_services_df = services[
                services["service_type_id"] == service_type_id
            ].copy()
            current_dist_normative = current_services_df[
                "radius_availability_meters"
            ].iloc[0]
            services_indexes = current_services_df.index.copy()
            access_matrix.loc[:, services_indexes] = pd.DataFrame(
                np.where(
                    access_matrix[services_indexes] > current_dist_normative, 0, 1
                ),
                index=access_matrix.index,
                columns=services_indexes,
            )
        return access_matrix

    def build_distance_matrix(
        self, buildings: gpd.GeoDataFrame, services: gpd.GeoDataFrame
    ) -> pd.DataFrame:
        """
        Function builds distance matrix by calculating distance in metres
        Args:
            buildings (gpd.GeoDataFrame): Building geometries
            services (gpd.GeoDataFrame): Service geometries with appropriate attributes
        Returns:
            pd.DataFrame: Distance matrix with binary representation of buildings-services distance access
        """

        distance_matrix = self.calculate_availability_matrix(
            buildings, services, services["radius_availability_meters"].max()
        )
        filtered_matrix = self.filter_matrix(distance_matrix, services)
        return filtered_matrix

    async def async_build_distance_matrix(
        self, buildings: gpd.GeoDataFrame, services: gpd.GeoDataFrame
    ) -> pd.DataFrame:
        """
        Function builds distance matrix by calculating distance in metres asynchronously
        Args:
            buildings (gpd.GeoDataFrame): Building geometries
            services (gpd.GeoDataFrame): Service geometries with appropriate attributes
        Returns:
            pd.DataFrame: Distance matrix with binary representation of buildings-services distance access
        """

        return await asyncio.to_thread(self.build_distance_matrix, buildings, services)


matrix_builder = MatrixBuilder()
