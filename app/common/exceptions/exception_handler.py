"""Exception handling middleware is defined here."""

import traceback

from fastapi import FastAPI, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.observability.metrics import Metrics

from .sirtep_exceptions import TaskNotFound


class ExceptionHandlerMiddleware(
    BaseHTTPMiddleware
):  # pylint: disable=too-few-public-methods
    """Handle exceptions, so they become http response code 500 - Internal Server Error if not handled as HTTPException
    previously.
    Attributes:
           app (FastAPI): The FastAPI application instance.
    """

    def __init__(self, app: FastAPI, metrics: Metrics):
        """
        Universal exception handler middleware init function.
        Args:
            app (FastAPI): The FastAPI application instance.
        """

        super().__init__(app)
        self.metrics = metrics

    @staticmethod
    async def prepare_request_info(request: Request) -> dict:
        """
        Function prepares request input data
        Args:
            request (Request): Request instance.
        Returns:
            dict: Request input data.
        """

        request_info = {
            "method": request.method,
            "url": str(request.url),
            "path_params": dict(request.path_params),
            "query_params": dict(request.query_params),
            "headers": dict(request.headers),
        }

        try:
            request_info["body"] = await request.json()
            return request_info
        except:
            try:
                request_info["body"] = str(await request.body())
                return request_info
            except:
                request_info["body"] = "Could not read request body"
                return request_info

    async def dispatch(self, request: Request, call_next):
        """
        Dispatch function for sending errors to user from API
        Args:
            request (Request): The incoming request object.
            call_next: function to extract.
        """

        try:
            return await call_next(request)
        except TaskNotFound as task_not_found:
            request_info = self.prepare_request_info(request)
            return JSONResponse(
                status_code=404,
                content={
                    "message": repr(task_not_found),
                    "error_type": task_not_found.__class__.__name__,
                    "request_info": request_info,
                    "input": task_not_found.input_repr(),
                    "traceback": traceback.format_exc().splitlines(),
                },
            )

        except Exception as e:
            request_info = await self.prepare_request_info(request)
            self.metrics.http.errors.add(
                1,
                {
                    "method": request.method,
                    "path": self._normalize_path(request),
                    "error_type": type(e).__name__,
                    "status_code": 500,
                },
            )
            return JSONResponse(
                status_code=500,
                content={
                    "message": "Internal server error",
                    "error_type": e.__class__.__name__,
                    "request": request_info,
                    "detail": str(e),
                    "traceback": traceback.format_exc().splitlines(),
                },
            )

    @staticmethod
    def _normalize_path(request: Request) -> str:
        """
        Normalize path to avoid high-cardinality metrics.
        """

        route = request.scope.get("route")

        if route and hasattr(route, "path"):
            return route.path

        return request.url.path
