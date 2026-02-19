from pathlib import Path

from iduconfig import Config
from loguru import logger

import app.dependencies as deps
from app.api_clients.urban_api_client import UrbanAPIClient
from app.common.api_handlers.json_api_handler import JSONAPIHandler
from app.common.logging.logger_conf import configure_logger
from app.common.sceduler.sceduler import Scheduler
from app.common.storage.sirtep_storage import SirtepStorage
from app.common.storage.storage_service import StorageService
from app.common.tasks.task_service import TaskService
from app.observability import OpenTelemetryAgent, PrometheusConfig
from app.sirtep.sirtep_service import SirtepService
from app.system_router.config.config_service import ConfigService


async def init_entities():
    """
    Initialize and configure all necessary entities for the application.
    This includes setting up logging, API clients, data parsers, storage services,
    task services, and schedulers.
    Each entity is configured using the provided configuration settings.
    """

    logger.info("Initializing entities")
    logger.info("Setting up app config instance")
    deps.config = Config()
    logger.info("Setting up logging")
    deps.log_path = Path(deps.config.get("LOG_NAME"))
    configure_logger(
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <b>{message}</b>",
        "INFO",
        log_path=deps.log_path,
    )
    logger.info("Starting app external config service")
    deps.config_service = ConfigService(deps.config)
    logger.info("Setting up api handlers")
    deps.urban_api_json_handler = JSONAPIHandler(deps.config.get("URBAN_API"))
    logger.info("Setting up api clients")
    deps.urban_api_client = UrbanAPIClient(deps.urban_api_json_handler)
    logger.info("Setting up sirtep parser")
    deps.sirtep_parser = deps.SirtepDataParser(deps.config)

    logger.info("Setting up sirtep storage")
    deps.matrix_storage = SirtepStorage(
        Path(deps.config.get("COMMON_CACHE")) / deps.config.get("MATRIX_CACHE"),
        deps.config,
    )
    deps.response_storage = SirtepStorage(
        Path(deps.config.get("COMMON_CACHE")) / deps.config.get("RESPONSE_CACHE"),
        deps.config,
    )
    deps.provision_storage = SirtepStorage(
        Path(deps.config.get("COMMON_CACHE")) / deps.config.get("PROVISION_CACHE"),
        deps.config,
    )
    deps.storage_service = StorageService(
        deps.config,
        matrix_storage=deps.matrix_storage,
        response_storage=deps.response_storage,
        provision_storage=deps.provision_storage,
    )
    logger.info("Setting up task service")
    deps.task_service = TaskService()
    logger.info("Setting up sirtep service")
    deps.sirtep_service = SirtepService(
        deps.urban_api_client,
        deps.sirtep_parser,
        deps.storage_service,
        deps.task_service,
    )
    logger.info("Setting up scheduler")
    deps.scheduler = Scheduler(deps.config)
    await deps.scheduler.add_job(
        deps.storage_service.delete_irrelevant_cache, "interval"
    )


async def start_prometheus():
    """
    Start the prometheus server
    """

    logger.info(f"Starting Prometheus server on {deps.config.get('PROMETHEUS_PORT')}")
    deps.otel_agent = OpenTelemetryAgent(
        prometheus_config=PrometheusConfig(
            host="0.0.0.0",
            port=int(deps.config.get("PROMETHEUS_PORT")),
        ),
    )
    logger.info(f"Prometheus server started on {deps.config.get('PROMETHEUS_PORT')}")


async def shutdown_prometheus():
    """
    Shutdowns prometheus service
    """

    logger.info("Shutting down Prometheus server")
    deps.otel_agent.shutdown()
    logger.info("Prometheus server was shut down")
