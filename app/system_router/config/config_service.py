from iduconfig import Config

from app.common.exceptions.http_exception_wrapper import http_exception
from app.system_router.schemas import ConfigSchema


class ConfigService:
    """
    Service to manage environment variables in config
    Attributes:
        config (Config): config instance
    """

    def __init__(self, config: Config) -> None:
        """
        Service to manage environment variables in config
        Args:
            config (Config): config instance
        Returns:
            None
        """

        self.config = config

    async def get_env(self, env_name: str) -> ConfigSchema:
        """
        Function to get environment variable from config if exists
        Args:
            env_name (str): environment variable name
        Returns:
            ConfigSchema: environment variable value or None if not exists
        Raises:
            HTTPException:
                - 500 if failed to get environment variable
        """

        try:
            env_value = self.config.get(env_name)
            return ConfigSchema(key=env_name, value=env_value)
        except Exception as e:
            raise http_exception(
                status_code=500,
                msg=f"Failed to get environment variable {env_name}",
                _input={"env_name": env_name},
                _detail={"error": repr(e)},
            ) from e

    async def reset_env(self, env_name: str, env_value: str) -> ConfigSchema:
        """
        Function to reset environment variable in config if exists
        Args:
            env_name (str): environment variable name
            env_value (str): environment variable value
        Returns:
            ConfigSchema: status of operation
        Raises:
            HTTPException:
                - 500 if environment variable does not exist or failed to set
        """

        if self.config.get(env_name):
            try:
                self.config.set(env_name, env_value)
                return ConfigSchema(key=env_name, value=env_value)
            except Exception as e:
                raise http_exception(
                    status_code=500,
                    msg=f"Failed to set environment variable {env_name}",
                    _input={"env_name": env_name, "env_value": env_value},
                    _detail={"error": repr(e)},
                ) from e
        raise http_exception(
            status_code=400,
            msg=f"Environment variable {env_name} does not exist",
            _input={"env_name": env_name, "env_value": env_value},
        )

    async def set_env(self, env_name: str, env_value: str) -> ConfigSchema:
        """
        Function to set environment variable in config
        Args:
            env_name (str): environment variable name
            env_value (str): environment variable value
        Returns:
            ConfigSchema: status of operation
        Raises:
            HTTPException:
                - 500 if failed to set environment variable
        """

        try:
            self.config.set(env_name, env_value)
            return ConfigSchema(key=env_name, value=env_value)
        except Exception as e:
            raise http_exception(
                status_code=500,
                msg=f"Failed to set environment variable {env_name}",
                _input={"env_name": env_name, "env_value": env_value},
                _detail={"error": repr(e)},
            ) from e
