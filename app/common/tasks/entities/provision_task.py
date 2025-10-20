from dataclasses import dataclass

from .base_task import BaseTask


@dataclass
class ProvisionTask(BaseTask):
    """
    Class for provision tasks. Inherits from BaseTask.
    Attributes:
        task_progress (float): Progress of the task in percentage
    """

    task_progress: float
