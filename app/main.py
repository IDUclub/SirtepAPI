from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse

from app.__version__ import APP_VERSION
from app.common.middlewares.exception_handler import ExceptionHandlerMiddleware
from app.common.middlewares.prometheus_handler import ObservabilityMiddleware
from app.init_entities import init_entities, shutdown_prometheus, start_prometheus
from app.observability.metrics import setup_metrics
from app.sirtep.sirtep_controller import sirtep_router
from app.system_router.system_controller import system_router

metrics = setup_metrics()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_entities()
    await start_prometheus()
    yield
    await shutdown_prometheus()


app = FastAPI(
    title="Sirtep API",
    lifespan=lifespan,
    description="API for scheduling project construction",
    version=APP_VERSION,
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ExceptionHandlerMiddleware, metrics=metrics)
app.add_middleware(ObservabilityMiddleware, metrics=metrics)
app.add_middleware(GZipMiddleware, minimum_size=100)


@app.get("/", include_in_schema=False)
async def read_root():
    return RedirectResponse("/docs")


app.include_router(system_router)
app.include_router(sirtep_router)
