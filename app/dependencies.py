from pathlib import Path

from iduconfig import Config

from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.api_handlers.json_api_handler import JSONAPIHandler
from app.common.logging.logger_conf import configure_logger
from app.common.parsing.sirtep_data_parser import SirtepDataParser
from app.sirtep.sirtep_service import SirtepService

absolute_app_path = Path().absolute()
config = Config()

configure_logger(
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <b>{message}</b>",
    "INFO",
    absolute_app_path / config.get("LOG_NAME"),
)

urban_api_json_handler = JSONAPIHandler(config.get("URBAN_API"))
urban_api_gateway = UrbanAPIClient(urban_api_json_handler)
sirtep_parser = SirtepDataParser(config)
sirtep_service = SirtepService(urban_api_gateway, sirtep_parser)
