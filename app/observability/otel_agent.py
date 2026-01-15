"""Open Telemetry agent initialization is defined here"""

import platform
from functools import cache

from opentelemetry import metrics, trace
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import (
    SERVICE_INSTANCE_ID,
    SERVICE_NAME,
    SERVICE_VERSION,
    Resource,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.__version__ import APP_VERSION

from .config import JaegerConfig, PrometheusConfig
from .metrics_server import PrometheusServer


@cache
def get_resource() -> Resource:
    return Resource.create(
        attributes={
            SERVICE_NAME: "SirtepAPI",
            SERVICE_VERSION: APP_VERSION,
            SERVICE_INSTANCE_ID: platform.node(),
        }
    )


class OpenTelemetryAgent:  # pylint: disable=too-few-public-methods
    def __init__(
        self,
        prometheus_config: PrometheusConfig | None,
        jaeger_config: JaegerConfig | None,
    ):
        self._resource = get_resource()
        self._prometheus: PrometheusServer | None = None

        if prometheus_config is not None:
            self._prometheus = PrometheusServer(
                port=prometheus_config.port, host=prometheus_config.host
            )

            reader = PrometheusMetricReader()
            provider = MeterProvider(resource=self._resource, metric_readers=[reader])
            metrics.set_meter_provider(provider)

        if jaeger_config is not None:
            tracer_provider = TracerProvider(resource=self._resource)
            tracer_provider.add_span_processor(processor)
            trace.set_tracer_provider(tracer_provider)

    def shutdown(self) -> None:
        """Stop metrics and tracing services if they were started."""
        if self._prometheus is not None:
            self._prometheus.shutdown()
