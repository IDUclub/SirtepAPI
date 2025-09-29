from typing import Annotated

from fastapi import APIRouter, Depends

from app.common.auth.auth import verify_token
from app.dependencies import sirtep_service

from .dto import SchedulerDTO
from .schema import (
    SchedulerOptimizationSchema,
)

sirtep_router = APIRouter(prefix="/optimize", tags=["OPTIMIZATION"])


@sirtep_router.get("/scheduler", response_model=SchedulerOptimizationSchema)
async def get_scheduler(
    params: Annotated[SchedulerDTO, Depends(SchedulerDTO)],
    token: str = Depends(verify_token),
) -> SchedulerOptimizationSchema:

    return await sirtep_service.calculate_schedule(params, token)
