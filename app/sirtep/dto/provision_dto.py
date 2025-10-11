from pydantic import field_validator

from app.common.exceptions.http_exception_wrapper import http_exception

from .sheduler_dto import SchedulerDTO


class ProvisionDTO(SchedulerDTO):
    """
    DTO class for calculating provision for project scenario.
    Attributes:
        Inherits all attributes from SchedulerDTO.

    """

    @field_validator("profile_id")
    @classmethod
    def validate_profile_id(cls, value: int) -> int:
        """
        Validates that the profile_id is greater than 0.
        Args:
            value (int): The profile_id to validate.
        Returns:
            int: The validated profile_id.
        Raises:
            ValueError: If the profile_id is not greater than 0.
        """
        if value not in (1, 2, 8):
            raise http_exception(
                400,
                msg="Invalid profile_id, must be one of [1, 2, 8]",
                _input={"profile_id": value},
                _detail={"available_profile_ids": [1, 2, 8]},
            )
        return value
