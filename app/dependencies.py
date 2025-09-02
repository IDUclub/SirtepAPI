import sys
from pathlib import Path

from iduconfig import Config
from loguru import logger

from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.api_handlers.json_api_handler import JSONAPIHandler
from app.common.logging.logger_conf import configure_logger

absolute_app_path = Path().absolute()
config = Config()

configure_logger(
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <b>{message}</b>",
    "INFO",
    absolute_app_path / config.get("LOG_NAME"),
)

urban_api_json_handler = JSONAPIHandler(config.get("URBAN_API"))
urban_api_gateway = UrbanAPIClient(urban_api_json_handler)
