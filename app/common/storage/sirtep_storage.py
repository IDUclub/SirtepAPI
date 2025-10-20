import datetime
from pathlib import Path

from iduconfig import Config
from idustorage import Storage
from loguru import logger


class SirtepStorage(Storage):
    """
    Storage class for Sirtep data caching and retrieval
    Inherits from Storage class of idustorage
    """

    def __init__(self, cache_path: Path, config: Config, separator: str = "_"):

        super().__init__(cache_path, config, separator)

    def retrieve_cached_file(self, pattern: str, ext: str, *args) -> str:
        """
        Get filename of the most recent file created of such type.

        :param pattern: rather a name of the file.
        :param ext: extension of the file.
        :param args: specification for the file to distinguish between.

        :return: filename of the most recent file created of such type if it's in the span of actuality.
        """

        files = [
            file.name
            for file in self.cache_path.glob(
                f"*{pattern}{''.join([f'{self.separator}{arg}' for arg in args])}*{ext}"
            )
        ]
        files.sort(reverse=True)
        logger.info(f"Found files for pattern {pattern} with args {args}: {files}")
        actual_filename: str = ""
        for file in files:
            broken_filename = file.split(self.separator)
            date = datetime.datetime.strptime(broken_filename[0], "%Y-%m-%d-%H-%M-%S")
            hours_diff = (datetime.datetime.now() - date).total_seconds() // 3600
            if hours_diff < int(self.config.get("actuality")):
                actual_filename = file
                logger.info(f"Found cached file - {actual_filename}")
                break
        return actual_filename
