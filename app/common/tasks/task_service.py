from datetime import datetime
from typing import Literal

from .entities import ProvisionTask


class TaskService:

    def __init__(self):

        self.tasks = {}

    def create_task(self, name: Literal["tep"], *args: str):

        if name == "tep":
            task = ProvisionTask(
                task_id="_".join(args),
                status="in_queue",
                start_time=datetime.now(),
                details={},
                task_progress=0.0,
            )
            self.tasks[task.task_id] = task
            return task.task_id
        raise NotImplementedError(f"Task {name} not implemented")

    def set_task_attribute(
        self,
        task_id: str,
        attribute: str,
        value: (
            Literal["in_queue", "pending", "completed", "failed"]
            | float
            | int
            | str
            | dict
        ),
    ):

        if attribute == "task_id":
            raise AttributeError("Cannot modify task_id attribute")
        if task_id in self.tasks:
            if hasattr(self.tasks[task_id], attribute):
                setattr(self.tasks[task_id], attribute, value)
            else:
                raise AttributeError(
                    f"Attribute {attribute} not found in task {task_id}"
                )
        else:
            raise KeyError(f"Task {task_id} not found")

    def get_task(self, task_id: str) -> ProvisionTask:

        if task_id in self.tasks:
            return self.tasks[task_id]
        else:
            raise KeyError(f"Task {task_id} not found")

    def delete_task(self, task_id: str):
        if task_id in self.tasks:
            del self.tasks[task_id]
        else:
            raise KeyError(f"Task {task_id} not found")
