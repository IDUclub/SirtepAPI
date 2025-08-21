from typing import Annotated

from fastapi import APIRouter, Depends

from .dto import SchedulerDTO

sirtep_router = APIRouter(prefix="optimize")


@sirtep_router.get("/scheduler", response_model=dict)
async def get_scheduler(
    params: Annotated[SchedulerDTO, Depends(SchedulerDTO)],
):

    pass
