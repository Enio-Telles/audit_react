from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

try:
    from prometheus_client import Counter, Histogram
except Exception:  # pragma: no cover - dependencia opcional
    Counter = None
    Histogram = None


PIPELINE_STEP_DURATION = (
    Histogram(
        "audit_react_pipeline_step_duration_seconds",
        "Duracao das etapas instrumentadas do pipeline",
        labelnames=("step",),
    )
    if Histogram
    else None
)
PIPELINE_STEP_ERRORS = (
    Counter(
        "audit_react_pipeline_step_errors_total",
        "Erros observados em etapas instrumentadas",
        labelnames=("step",),
    )
    if Counter
    else None
)
PIPELINE_RECORDS_PROCESSED = (
    Counter(
        "audit_react_pipeline_records_processed_total",
        "Quantidade de registros processados em etapas instrumentadas",
        labelnames=("step",),
    )
    if Counter
    else None
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for attr in ("step", "duration_seconds", "records_processed", "error_type"):
            if hasattr(record, attr):
                payload[attr] = getattr(record, attr)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


@dataclass(frozen=True)
class StepObservation:
    step: str
    duration_seconds: float
    records_processed: int | None = None


def configure_structured_logging(logger_name: str = "audit_react.observabilidade") -> logging.Logger:
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def observe_step(step_name: str, *, logger_name: str = "audit_react.pipeline") -> Callable[[F], F]:
    logger = configure_structured_logging(logger_name)

    def decorator(func: F) -> F:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            started_at = time.perf_counter()
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                duration = time.perf_counter() - started_at
                if PIPELINE_STEP_DURATION:
                    PIPELINE_STEP_DURATION.labels(step=step_name).observe(duration)
                if PIPELINE_STEP_ERRORS:
                    PIPELINE_STEP_ERRORS.labels(step=step_name).inc()
                logger.exception(
                    "falha em etapa instrumentada",
                    extra={
                        "step": step_name,
                        "duration_seconds": round(duration, 6),
                        "error_type": exc.__class__.__name__,
                    },
                )
                raise
            duration = time.perf_counter() - started_at
            if PIPELINE_STEP_DURATION:
                PIPELINE_STEP_DURATION.labels(step=step_name).observe(duration)
            logger.info(
                "etapa instrumentada concluida",
                extra={"step": step_name, "duration_seconds": round(duration, 6)},
            )
            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def observe_records(step_name: str, records_processed: int) -> None:
    logger = configure_structured_logging("audit_react.pipeline.records")
    if PIPELINE_RECORDS_PROCESSED:
        PIPELINE_RECORDS_PROCESSED.labels(step=step_name).inc(records_processed)
    logger.info(
        "registros processados",
        extra={"step": step_name, "records_processed": records_processed},
    )
