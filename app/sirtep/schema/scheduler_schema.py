from pydantic import BaseModel


class SchedulerProvisionSchema(BaseModel):

    house_construction_period: dict
    service_construction_period: dict
    houses_per_period: list[float | None]
    services_per_period: list[float | None]
    houses_area_per_period: list[float | None]
    services_area_per_period: list[float | None]
    provided_per_period: list[float | None]
    periods: list[int | None]


class SchedulerSimpleSchema(BaseModel):

    id: list[int | str]
    period: list[int | None]
    percent_built: list[float | None]
    area: list[float | None]
    priority: list[int | None]


class SchedulerOptimizaionSchema(BaseModel):

    provision: SchedulerProvisionSchema | None
    simple: SchedulerSimpleSchema | None
