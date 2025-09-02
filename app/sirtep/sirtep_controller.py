from typing import Annotated

from fastapi import APIRouter, Depends

from app.common.auth.auth import verify_token

from .dto import SchedulerDTO
from .schema import (
    SchedulerOptimizaionSchema,
)
from .sirtep_service import sirtep_service

sirtep_router = APIRouter(prefix="/optimize", tags=["OPTIMIZATION"])


@sirtep_router.get("/scheduler", response_model=SchedulerOptimizaionSchema)
async def get_scheduler(
    params: Annotated[SchedulerDTO, Depends(SchedulerDTO)],
    token: str = Depends(verify_token),
) -> SchedulerOptimizaionSchema:

    return await sirtep_service.calculate_schedule(params, token)
