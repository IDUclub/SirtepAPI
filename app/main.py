from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import RedirectResponse

from app.common.exceptions.exception_handler import ExceptionHandlerMiddleware
from app.sirtep.sirtep_controller import sirtep_router
from app.system_router.system_controller import system_router

app = FastAPI(
    title="Sertep API",
    description="API for scheduling project construction",
    version="0.1.0",
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=100)
app.add_middleware(ExceptionHandlerMiddleware)


@app.get("/", include_in_schema=False)
async def read_root():
    return RedirectResponse("/docs")


app.include_router(system_router)
app.include_router(sirtep_router)
