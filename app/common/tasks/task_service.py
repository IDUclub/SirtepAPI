from datetime import datetime
from functools import wraps
from typing import Any, Literal

from .entities import ProvisionTask, TaskType


class TaskService:

    def check_task(fn):
        @wraps(fn)
        def wrapper(self, task_id: str, *args, **kwargs):
            if task_id not in self.tasks:
                raise KeyError(f"Task {task_id} not found")
            return fn(self, task_id, *args, **kwargs)

        return wrapper

    def __init__(self):

        self.tasks = {}

    def create_task(self, name: TaskType, *args: str) -> str:
        """
        Function creates task for given type
        Args:
            name (TaskType): task type, only "tep" us currently supported
        Raises:
            NotImplementedError: if task type not implemented
        Returns:
            str: unique task id
        """

        if name == TaskType.TEP:
            task = ProvisionTask(
                task_id="_".join(args),
                status="in_queue",
                start_time=datetime.now(),
                details=None,
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

        if hasattr(self.tasks[task_id], attribute):
            setattr(self.tasks[task_id], attribute, value)
        else:
            raise AttributeError(f"Attribute {attribute} not found in task {task_id}")

    @check_task
    def set_task_status(self, task_id: str, status: str) -> None:
        """
        Function sets task status to given value
        Args:
            task_id (str): unique task id
            status (str): task status to be set
        Returns:
            None
        Raises:
            KeyError: if task id not found in tasks
            AttributeError: if task chosen attribute doesn't exist
        """

        return self.__set_task_attribute__(task_id, "status", status)

    @check_task
    def set_task_progress(self, task_id: str, progress: float) -> None:
        """
        Function sets task progress to given value
        Args:
            task_id (str): unique task id
            progress (float): task progress to be set
        Returns:
            None
        Raises:
            KeyError: if task id not found in tasks
            AttributeError: if task chosen attribute doesn't exist
        """

        return self.__set_task_attribute__(task_id, "task_progress", progress)

    @check_task
    def set_task_details(self, task_id: str, details: Any) -> None:
        """
        Function sets task details to given value
        Args:
            task_id (str): unique task id
            details (Any): task details to be set
        Returns:
            None
        Raises:
            KeyError: if task id not found in tasks
            AttributeError: if task chosen attribute doesn't exist
        """

        return self.__set_task_attribute__(task_id, "details", details)

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
