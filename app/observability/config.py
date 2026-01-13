"""Observability config is defined here."""

from dataclasses import dataclass


@dataclass
class ExporterConfig:
    endpoint: str
    tls_insecure: bool = False


@dataclass
class PrometheusConfig:
    host: str
    port: int


@dataclass
class JaegerConfig:
    endpoint: str


@dataclass
class ObservabilityConfig:
    prometheus: PrometheusConfig | None = None
    jaeger: JaegerConfig | None = None
