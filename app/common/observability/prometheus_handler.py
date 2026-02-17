"""Observability middleware is defined here."""

import time
from typing import Callable

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from app.observability.metrics import Metrics


class ObservabilityMiddleware(BaseHTTPMiddleware):
    """
    Production-grade observability middleware.

    Collects:
        - request latency histogram
        - started / finished counters
        - inflight gauge
        - error counters (4xx / 5xx / unhandled exceptions)
    """

    def __init__(self, app: FastAPI, metrics: Metrics):
        super().__init__(app)
        self._http_metrics = metrics.http

    async def dispatch(self, request: Request, call_next: Callable) -> Response:

        method = request.method
        path = self._normalize_path(request)
        self._http_metrics.requests_started.add(
            1,
            {"method": method, "path": path},
        )
        self._http_metrics.inflight_requests.add(1)
        start_time = time.monotonic()

        try:
            response = await call_next(request)
        except Exception as exc:
            duration = time.monotonic() - start_time

            self._http_metrics.request_processing_duration.record(
                duration,
                {"method": method, "path": path},
            )
            self._http_metrics.errors.add(
                1,
                {
                    "method": method,
                    "path": path,
                    "error_type": type(exc).__name__,
                    "status_code": 500,
                },
            )
            self._http_metrics.inflight_requests.add(-1)
            raise

        duration = time.monotonic() - start_time
        self._http_metrics.request_processing_duration.record(
            duration,
            {"method": method, "path": path},
        )
        self._http_metrics.requests_finished.add(
            1,
            {
                "method": method,
                "path": path,
                "status_code": response.status_code,
            },
        )
        if response.status_code >= 500:
            self._http_metrics.errors.add(
                1,
                {
                    "method": method,
                    "path": path,
                    "error_type": "server_error",
                    "status_code": response.status_code,
                },
            )
        elif response.status_code >= 400:
            self._http_metrics.errors.add(
                1,
                {
                    "method": method,
                    "path": path,
                    "error_type": "client_error",
                    "status_code": response.status_code,
                },
            )

        self._http_metrics.inflight_requests.add(-1)
        return response

    @staticmethod
    def _normalize_path(request: Request) -> str:
        """
        Normalize path to avoid high-cardinality metrics.
        """

        route = request.scope.get("route")

        if route and hasattr(route, "path"):
            return route.path

        return request.url.path
