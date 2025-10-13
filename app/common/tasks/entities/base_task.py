from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class TaskStatus(StrEnum):
    """
    Class for handling task status
    Attributes:
        IN_QUEUE (str): "in_queue" status, if task execution haven't started
        PENDING (str): "pending" status, if task is executing at the current moment
        COMPLETED (str): "completed" status, if task successfully completed
        FAILED (str): "failed" status, if task occured an error during execution
    """

    IN_QUEUE: str = "in_queue"
    PENDING: str = "pending"
    COMPLETED: str = "complited"
    FAILED: str = "failed"


@dataclass
class BaseTask:
    """
    Base class for tasks
    Attributes:
        task_id (str): Unique identifier for the task
        status (TaskStatus): Status equals to in_queue
        start_time (datetime): Time when the task was started
        details (Any): Additional details about the task in json serializable format
    """

    task_id: str
    status: TaskStatus
    start_time: datetime
    details: Any
