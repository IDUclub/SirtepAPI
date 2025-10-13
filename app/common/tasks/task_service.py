from datetime import datetime
from typing import Any, Literal

from .entities import ProvisionTask


class TaskService:

    def __init__(self):

        self.tasks = {}

    def create_task(self, name: Literal["tep"], *args: str):
        """
        Function creates task for given type
        Args:
            name (Literal[&quot;tep&quot;]): _description_

        Raises:
            NotImplementedError: _description_

        Returns:
            _type_: _description_
        """

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

    def __set_task_attribute__(
        self,
        task_id: str,
        attribute: str,
        value: Any,
    ):
        """
        Function sets task attribute to given value

        Args:
            task_id (str): unique task id
            attribute (str): name of attribute to be set
            value (Any): attribute value to set to

        Raises:
            KeyError: if task id not found in tasks
            AttributeError: if task doesn't have provided task id
        """

        if task_id not in self.tasks:
            raise KeyError(f"Task {task_id} not found")
        if hasattr(self.tasks[task_id], attribute):
            setattr(self.tasks[task_id], attribute, value)
        else:
            raise AttributeError(f"Attribute {attribute} not found in task {task_id}")

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
