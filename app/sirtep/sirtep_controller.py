from typing import Annotated, Union

from fastapi import APIRouter, Depends

import app.dependencies as deps
from app.common.auth.auth import verify_token

from .dto import SchedulerDTO
from .schema import (
    ProvisionInProgressSchema,
    ProvisionSchema,
    SchedulerOptimizationSchema,
)

sirtep_router = APIRouter(prefix="/optimize", tags=["OPTIMIZATION"])


@sirtep_router.get("/scheduler", response_model=SchedulerOptimizationSchema)
async def get_scheduler(
    params: Annotated[SchedulerDTO, Depends(SchedulerDTO)],
    token: str = Depends(verify_token),
) -> SchedulerOptimizationSchema:

    return await deps.sirtep_service.calculate_schedule(params, token)


@sirtep_router.get(
    "/teps", response_model=Union[ProvisionSchema, ProvisionInProgressSchema]
)
async def get_teps(
    params: Annotated[SchedulerDTO, Depends(SchedulerDTO)],
    token: str = Depends(verify_token),
) -> ProvisionSchema | ProvisionInProgressSchema:

    return await deps.sirtep_service.get_provision_for_request(params, token)
