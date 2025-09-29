from pydantic import BaseModel, field_validator


class SchedulerProvisionSchema(BaseModel):

    house_construction_period: dict
    service_construction_period: dict
    houses_per_period: list[float | None]
    services_per_period: list[float | None]
    houses_area_per_period: list[float | None]
    services_area_per_period: list[float | None]
    provided_per_period: list[float | None]
    periods: list[int | None]
    buildings_comment: str | None
    services_comment: str | None

    @field_validator("houses_per_period", "services_per_period", "houses_area_per_period", "services_area_per_period", "provided_per_period", mode="before")
    @classmethod
    def round_values(cls, value: list[float | None]) -> list[float | None]:
        """
        Function rounds float response model fields to 2 digits after float,
        Args:
            value (float | None): float value
        Returns:
            float | None: rounded float value or None
        """

        return [round(i, 2) if i else i for i in value]


class SchedulerSimpleSchema(BaseModel):

    id: list[int | str]
    period: list[int | None]
    percent_built: list[float | None]
    area: list[float | None]
    priority: list[int | None]

    @field_validator("percent_built", mode="before")
    @classmethod
    def convert_to_percent(cls, value: list[float | None]) -> list[float | None]:
        """
        Function converts partial to float percent and rounds by 2 digits after float
        Args:
            value (list[float | None]): partial value and rounds by 2 digits after float
        Returns:
            list[float | None]: converted to percent values and rounded by 2 digits after float
        """

        return [round(i * 100, 2) if i else i for i in value]

    @field_validator("area", mode="before")
    @classmethod
    def round_values(cls, value: list[float | None]) -> list[float | None]:
        """
        Function rounds float values to 2 digits after float
        Args:
            value (list[float | None]): float value
        Returns:
            list[float | None]: values rounded by 2 digits after float
        """

        return [round(i, 2) if i else i for i in value]


class SchedulerOptimizationSchema(BaseModel):

    provision: SchedulerProvisionSchema | None
    simple: SchedulerSimpleSchema | None
