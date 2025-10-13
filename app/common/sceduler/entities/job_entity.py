from enum import Enum, unique


@unique
class JobType(Enum):
    """
    Enum class for scheduler job type
    Attributes:
        interval (str): default to "interval"
    """

    INTERVAL: str = "interval"
