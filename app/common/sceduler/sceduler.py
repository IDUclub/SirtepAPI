from typing import Callable, Literal

from apscheduler.schedulers.background import BackgroundScheduler


class Scheduler:

    def __init__(self) -> None:
        """
        Initializes the Scheduler with a background scheduler.
        """

        self.scheduler = BackgroundScheduler()

    async def add_djob(
        self, func: Callable, type: Literal["interval"], time: int
    ) -> None:

        try:
            self.scheduler.add_job(func, type, minutes=time)
        except Exception as e:
            raise e

    async def start(self):

        await self.scheduler.start()
