from enum import StrEnum


@unique
class JobType(StrEnum):
    """
    Enum class for scheduler job type
    Attributes:
        INTERVAL (str): default to "interval"
    """

    INTERVAL: str = "interval"
