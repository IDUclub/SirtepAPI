from pydantic import BaseModel


class ConfigSchema(BaseModel):
    """
    Schema for configuration settings.

    Fields:
        - key (str): The configuration key.
        - value (str): The configuration value.
    """

    key: str
    value: str
