import unittest

import geopandas as gpd
from shapely.geometry import Polygon

from app.common.parsing.sirtep_data_parser import SirtepDataParser


class StubConfig:
    VALUES = {
        "DEFAULT_FLOORS": "3",
        "LIVING_AREA_COEF": "1",
        "METRES_PER_HUMAN": "20",
    }

    def get(self, key):
        return self.VALUES[key]


class SirtepDataParserTestCase(unittest.TestCase):
    def test_parse_living_buildings_handles_mixed_building_records(self):
        buildings = gpd.GeoDataFrame(
            {
                "physical_objects": [
                    [{"physical_object_id": 1, "building": None}],
                    [
                        {
                            "physical_object_id": 2,
                            "building": {"floors": 5},
                        }
                    ],
                ],
                "geometry": [
                    Polygon([(0, 0), (0, 0.001), (0.001, 0.001), (0.001, 0)]),
                    Polygon([(0.002, 0), (0.002, 0.001), (0.003, 0.001), (0.003, 0)]),
                ],
            },
            crs=4326,
        )

        result = SirtepDataParser(StubConfig()).parse_living_buildings(buildings)

        self.assertEqual(result.loc[1, "floors"], 5)
        self.assertEqual(result.loc[2, "floors"], 5)


if __name__ == "__main__":
    unittest.main()
