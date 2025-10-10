from pathlib import Path

from iduconfig import Config

from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.api_handlers.json_api_handler import JSONAPIHandler
from app.common.logging.logger_conf import configure_logger
from app.common.parsing.sirtep_data_parser import SirtepDataParser
from app.common.sceduler.sceduler import Scheduler
from app.common.storage.sirtep_storage import SirtepStorage
from app.common.storage.storage_service import StorageService
from app.common.tasks.task_service import TaskService
from app.sirtep.sirtep_service import SirtepService

absolute_app_path = Path().absolute()
config = Config()

configure_logger(
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <b>{message}</b>",
    "INFO",
    absolute_app_path / config.get("LOG_NAME"),
)

urban_api_json_handler = JSONAPIHandler(config.get("URBAN_API"))
urban_api_client = UrbanAPIClient(urban_api_json_handler)
sirtep_parser = SirtepDataParser(config)

matrix_storage = SirtepStorage(
    Path().absolute() / config.get("COMMON_CACHE") / config.get("MATRIX_CACHE"), config
)
response_storage = SirtepStorage(
    Path().absolute() / config.get("COMMON_CACHE") / config.get("RESPONSE_CACHE"),
    config,
)
provision_storage = SirtepStorage(
    Path().absolute() / config.get("COMMON_CACHE") / config.get("PROVISION_CACHE"),
    config,
)

storage_service = StorageService(
    config,
    matrix_storage=matrix_storage,
    response_storage=response_storage,
    provision_storage=provision_storage,
)

task_service = TaskService()
sirtep_service = SirtepService(
    urban_api_client, sirtep_parser, storage_service, task_service
)
scheduler = Scheduler(config)
scheduler.add_job(storage_service.delete_irrelevant_cache, "interval")
