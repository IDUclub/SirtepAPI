class TaskNotFound(KeyError):

    def __init__(self, task_id: str, available_tasks: dict):

        self.task_id = task_id
        self.available_tasks = available_tasks

    def __str__(self):

        return f"Task {self.task_id} not found"

    def input_repr(self) -> dict:

        return {
            "task_id": self.task_id,
            "available_tasks": self.available_tasks,
        }
