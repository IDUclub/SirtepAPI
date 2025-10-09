from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass
class BaseTask:
    """
    Base class for tasks
    Attributes:
        task_id (str): Unique identifier for the task
        status (Literal["in_queue", "pending", "completed", "failed"]): Status
        start_time (datetime): Time when the task was started
        details (dict): Additional details about the task
    """

    task_id: str
    status: Literal["in_queue", "pending", "completed", "failed"]
    start_time: datetime
    details: dict
