from pydantic import BaseModel, Field


class SchedulerDTO(BaseModel):
    """
    DTO class for calculating construction schedule for project scenario.

    Fields:
        - scenario_id (int): The project scenario id to run optimization on.
        - profile_id (int): The project profile id to run optimization on.
        - periods (int): The number of  period of the optimization. Highly recommended to use quarters.
        SSERs AND TEPs will be calculated for each 5-th part of all periods.
        - max_area_per_period (int): The maximum area of the project to be considered.
    """

    scenario_id: int = Field(
        ge=0, description="The project scenario id to run optimization on."
    )
    profile_id: int = Field(
        ge=0, description="The project profile id to run optimization on."
    )
    periods: int = Field(
        ge=0,
        description="The number of  period of the optimization. Highly recommended to use quarters. SSERs AND TEPs will be calculated for each 5-th part of all periods.",
    )
    max_area_per_period: int = Field(
        ge=0, description="Construction speed for scenario"
    )

    def request_params_as_list(self) -> list[int]:
        """
        Converts the DTO fields to a list of their values.
        Returns:
            list[int]: List of DTO field values.
        """

        return [
            self.scenario_id,
            self.profile_id,
            self.periods,
            self.max_area_per_period,
        ]

    def request_params_as_dict(self) -> dict[str, int]:
        """
        Converts the DTO fields to a dictionary of their values.
        Returns:
            dict[str, int]: Dictionary of DTO field values.
        """

        return self.model_dump()
