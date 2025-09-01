from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.common.exceptions.http_exception_wrapper import http_exception
from app.dependencies import absolute_app_path, config

from .config import config_service
from .schemas import ConfigSchema

LOGS_PATH = absolute_app_path / f"{config.get('LOG_NAME')}"
system_router = APIRouter(prefix="/system", tags=["System"])


# TODO use structlog instead of loguru
@system_router.get("/logs")
async def get_logs():
    """
    Get logs file from app
    """

    try:
        return FileResponse(
            LOGS_PATH,
            media_type="application/octet-stream",
            filename=f"effects.log",
        )
    except FileNotFoundError as e:
        raise http_exception(
            status_code=404,
            msg="Log file not found",
            _input={"log_path": LOGS_PATH, "log_file_name": config.get("LOG_NAME")},
            _detail={"error": repr(e)},
        ) from e
    except Exception as e:
        raise http_exception(
            status_code=500,
            msg="Internal server error during reading logs",
            _input={"log_path": LOGS_PATH, "log_file_name": config.get("LOG_NAME")},
            _detail={"error": repr(e)},
        ) from e


@system_router.get("/config", response_model=ConfigSchema)
async def get_config(key: str) -> ConfigSchema:
    """
    Get current config as dict

    Params:

        - key (str): config key
    """

    return await config_service.get_config(key)


@system_router.post("/config", response_model=ConfigSchema, status_code=201)
async def set_config(key: str, value: str) -> ConfigSchema:
    """
    Set config key to value

    Params:

        - key (str): config key
        - value (str): config value
    """

    return await config_service.set_config(key, value)


@system_router.patch("/config", response_model=ConfigSchema, status_code=201)
async def reset_config(key: str, value: str) -> ConfigSchema:
    """
    Reset config key to value if key exists

    Params:

        - key (str): config key
        - value (str): config value
    """

    return await config_service.reset_config(key, value)
