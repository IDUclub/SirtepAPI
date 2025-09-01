from typing import Annotated

from fastapi import APIRouter, Depends

from app.common.auth.auth import verify_token

from .dto import SchedulerDTO
from .schema import (
    SchedulerOptimizaionSchema,
    SchedulerProvisionSchema,
    SchedulerSimpleSchema,
)
from .sirtep_service import sirtep_service

sirtep_router = APIRouter(prefix="/optimize")


@sirtep_router.get("/scheduler", response_model=SchedulerOptimizaionSchema)
async def get_scheduler(
    params: Annotated[SchedulerDTO, Depends(SchedulerDTO)],
    token: str = Depends(verify_token),
) -> SchedulerOptimizaionSchema:
    """
    Optimization of building construction schedule based on provided parameters.

    Params:

        - scenario_id (int): ID of the project scenario to optimize.

        - profile_id (int): ID of the project profile to optimize for (e.g. base or transport).

        - periods (int): Number of periods to optimize the schedule for.

        - max_area_per_period (float): Maximum area that can be constructed in each period in metres.

    Returns:

        SchedulerOptimizaionSchema: Schema with optimization results either containing provision or simple schedule
        (depends on profile).
    """

    return await sirtep_service.calculate_schedule(params, token)
