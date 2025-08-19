import json
from typing import Any, Dict, Literal, Optional

import geopandas as gpd
import pandas as pd
import shapely
from loguru import logger

from app.common.api_handlers.json_api_handler import JSONAPIHandler
from app.common.exceptions.http_exception_wrapper import http_exception


class UrbanAPIGateway:

    def __init__(self, base_url: str) -> None:
        self.json_handler = JSONAPIHandler(base_url)
        self.__name__ = "UrbanAPIGateway"
