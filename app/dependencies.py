from pathlib import Path

from iduconfig import Config

from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.api_handlers.json_api_handler import JSONAPIHandler
from app.common.parsing.sirtep_data_parser import SirtepDataParser
from app.common.sceduler.sceduler import Scheduler
from app.common.storage.sirtep_storage import SirtepStorage
from app.common.storage.storage_service import StorageService
from app.common.tasks.task_service import TaskService
from app.observability import OpenTelemetryAgent
from app.sirtep.sirtep_service import SirtepService
from app.system_router.config.config_service import ConfigService

config: Config | None = None
config_service: ConfigService | None = None
log_path: Path | None = None
urban_api_json_handler: JSONAPIHandler | None = None
urban_api_client: UrbanAPIClient | None = None
sirtep_parser: SirtepDataParser | None = None
matrix_storage: SirtepStorage | None = None
response_storage: SirtepStorage | None = None
provision_storage: SirtepStorage | None = None
storage_service: StorageService | None = None
task_service: TaskService | None = None
sirtep_service: SirtepService | None = None
scheduler: Scheduler | None = None
otel_agent: OpenTelemetryAgent | None = None
