from typing import Callable, Literal

from apscheduler.schedulers.background import BackgroundScheduler
from iduconfig import Config
from loguru import logger

from .entities import JobType


class Scheduler:
    """
    Scheduler class to manage background jobs using APScheduler.
    Attributes:
        scheduler (BackgroundScheduler): Instance of BackgroundScheduler
        config (Config): Configuration object containing settings
    """

    def __init__(self, config: Config) -> None:
        """
        Initializes the Scheduler with a background scheduler.
        Args:
            config (Config): Configuration object containing settings
        """

        self.scheduler = BackgroundScheduler()
        self.config = config

    async def add_job(self, func: Callable, job_type: JobType) -> None:
        """
        Function adds a scheduler job to scheduler from BackgroundScheduler
        Args:
            func (Callable): func to add to scheduler
            job_type (JobType): _description_
        """

        try:
            self.scheduler.add_job(
                func, job_type, minutes=int(self.config.get("ACTUALITY"))
            )
        except Exception as e:
            logger.error(e)
            raise e

    async def start(self):
        """
        Function runs dceduler
        Raises:
            e: any exception from sceduler
        """

        try:
            await self.scheduler.start()
        except Exception as e:
            logger.error(e)
            raise e
